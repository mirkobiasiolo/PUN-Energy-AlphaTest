import os
import time
import json
import subprocess
import xml.etree.ElementTree as ET
from pathlib import Path


class BatteryController:
    def __init__(self):
        # ====== PERCORSI FILE ======
        self.base_path = Path.home() / "Desktop" / "test 2025-12-05" / "dati"

        self.local_fromiDea_path = os.path.join(self.base_path, "FromiDea.xml")
        self.toiDea_path = os.path.join(self.base_path, "ToiDea.xml")
        self.guardrail_path = os.path.join(self.base_path, "guardrail_auto-consumo_locale.txt")
        self.schedule_path = os.path.join(self.base_path, "battery_schedule.json")
        self.meter_path = os.path.join(self.base_path, "Meter_schedule.json")
        # NUOVO: stato servizi DSO/Trading
        self.service_status_path = os.path.join(self.base_path, "service_status.txt")

        # ====== VALORI DI DEFAULT (se i file JSON non ci sono) ======
        self.capacity_kwh = 10.0
        self.emergency_start_soc_dec = 50   # 5%
        self.emergency_stop_soc_dec = 400   # 40%
        self.guardrail_1101_min = 0

        self.contatore_prelievo_w = 3000
        self.contatore_immissione_w = 3000

        # ====== PARAMETRI CARICA EMERGENZA ======
        self.ibat_low_min = 0
        self.ibat_low_max = 500

        self.ibat_min = 5000
        self.ibat_max = 6000

        self.step_emergency_1101 = 50

        self.emergency_1101_max = 0   # non andiamo in immissione in emergenza

        # stato interno
        self.emergency_active = False
        self.current_id = 0

        # valori letti da FromiDea
        self.values = {
            "1040": 0.0,  # SOC dec%
            "1013": 0.0,  # corrente batteria
            "1090": 0.0,  # sensore 0-10V
        }

        # Scheduler / configurazione servizi
        self.service_modes = {
            "carica_forzata_dso": "manual",
            "scarica_forzata_dso": "manual",
            "trading_scarica": "manual",
            "trading_carica": "manual",
        }
        self.started_services = set()

        self.schedule = self.load_schedule()
        self.load_meter_config()

        # Timer interno per meter (ogni 30 secondi)
        self.last_meter_time = 0.0

        # Avvio automatico eventuali servizi DSO/Trading
        self.auto_start_services()

    # =====================================================================
    #  UTILITIES DI PARSING FROMIDEA (TAG NUMERICI)
    # =====================================================================

    @staticmethod
    def _get_value_from_tag(xml_text: str, tag_name: str) -> float:
        try:
            open_tag = f"<{tag_name}>"
            close_tag = f"</{tag_name}>"

            start_index = xml_text.find(open_tag)
            if start_index == -1:
                return 0.0

            start_index += len(open_tag)
            end_index = xml_text.find(close_tag, start_index)
            if end_index == -1:
                return 0.0

            inner_text = xml_text[start_index:end_index].strip()
            inner_text = inner_text.replace(",", ".")
            return float(inner_text)
        except Exception:
            return 0.0

    # =====================================================================
    #  LETTURA FromiDea
    # =====================================================================

    def read_from_idea(self) -> bool:
        try:
            if not os.path.exists(self.local_fromiDea_path):
                print("FromiDea.xml non trovato.")
                return False

            with open(self.local_fromiDea_path, "r", encoding="utf-8") as f:
                xml_text = f.read()

            self.values["1040"] = self._get_value_from_tag(xml_text, "1040")
            self.values["1013"] = self._get_value_from_tag(xml_text, "1013")
            self.values["1090"] = self._get_value_from_tag(xml_text, "1090")

            soc = self.values["1040"]
            ibat = self.values["1013"]
            print(f"SOC(1040)={soc:.0f}  IBAT(1013)={ibat:.0f}")

            return True

        except Exception as ex:
            print("Errore lettura FromiDea:", ex)
            return False

    # =====================================================================
    #  SCRITTURA ToiDea (ID / IND / VAL)
    # =====================================================================

    def update_toi_dea(self, ind_value: int, val_value: int) -> None:
        try:
            if not os.path.exists(self.toiDea_path):
                print("ToiDea.xml non trovato.")
                return

            tree = ET.parse(self.toiDea_path)
            root = tree.getroot()

            self.current_id += 1
            if self.current_id >= 6000:
                self.current_id = 0

            id_node = root.find(".//ID")
            if id_node is not None:
                id_node.text = str(self.current_id)

            ind_node = root.find(".//IND")
            if ind_node is not None:
                ind_node.text = str(ind_value)

            val_node = root.find(".//VAL")
            if val_node is not None:
                val_node.text = str(val_value)

            tree.write(self.toiDea_path, encoding="utf-8", xml_declaration=True)

            print(f"ToiDea -> ID={self.current_id}  IND={ind_value}  VAL={val_value}")

        except Exception as ex:
            print("Errore aggiornamento ToiDea:", ex)

    # =====================================================================
    #  GUARDRAIL AUTOCONSUMO LOCALE
    # =====================================================================

    def write_guardrail_autoconsumo(self, enable_autoconsumo: bool) -> None:
        try:
            with open(self.guardrail_path, "w", encoding="utf-8") as f:
                f.write(f"AUTOCONSUMO={'1' if enable_autoconsumo else '0'}")
            print(f"Guardrail autoconsumo -> AUTOCONSUMO={'1' if enable_autoconsumo else '0'}")
        except Exception as ex:
            print("Errore scrittura guardrail:", ex)

    # =====================================================================
    #  SERVICE STATUS (DSO / TRADING ATTIVI?)
    # =====================================================================

    def read_service_active(self) -> bool:
        """
        Ritorna True se SERVICE=1 in service_status.txt.
        Se il file manca o non è leggibile, assume SERVICE=0 (nessun servizio).
        """
        try:
            if not os.path.exists(self.service_status_path):
                return False

            with open(self.service_status_path, "r", encoding="utf-8") as f:
                s = f.read().strip().upper()

            # accettiamo sia "1" che "SERVICE=1"
            if s == "1" or s.endswith("=1"):
                return True
            return False

        except Exception as ex:
            print("Errore lettura service_status.txt:", ex)
            return False

    # =====================================================================
    #  CARICA EMERGENZA
    # =====================================================================

    def handle_emergency_charge(self) -> None:
        soc = self.values["1040"]
        ibat = self.values["1013"]

        # 1) Ingresso in carica emergenza
        if (
            not self.emergency_active
            and soc == self.emergency_start_soc_dec
            and self.ibat_low_min <= ibat <= self.ibat_low_max
        ):
            print(">>> INGRESSO CARICA EMERGENZA <<<")
            self.emergency_active = True
            self.update_toi_dea(1102, 1)
            self.write_guardrail_autoconsumo(enable_autoconsumo=False)

        if not self.emergency_active:
            return

        # 2) Uscita
        if soc >= self.emergency_stop_soc_dec:
            print(">>> USCITA CARICA EMERGENZA <<<")
            self.emergency_active = False
            self.update_toi_dea(1102, 3)
            self.write_guardrail_autoconsumo(enable_autoconsumo=True)
            return

        # 3) Regolazione 1101
        current_1101 = self.current_1101_from_toi_dea()

        if ibat < self.ibat_min:
            nuovo_1101 = current_1101 - self.step_emergency_1101
        elif ibat > self.ibat_max:
            nuovo_1101 = current_1101 + self.step_emergency_1101
        else:
            print("Carica emergenza: 1013 nel range, nessuna variazione 1101.")
            return

        if nuovo_1101 < self.guardrail_1101_min:
            nuovo_1101 = self.guardrail_1101_min
        if nuovo_1101 > self.emergency_1101_max:
            nuovo_1101 = self.emergency_1101_max

        self.update_toi_dea(1101, int(nuovo_1101))

    def current_1101_from_toi_dea(self) -> int:
        try:
            if not os.path.exists(self.toiDea_path):
                return 0

            tree = ET.parse(self.toiDea_path)
            root = tree.getroot()
            val_node = root.find(".//VAL")
            if val_node is not None:
                return int(val_node.text.strip())
        except Exception as ex:
            print("Errore lettura 1101 da ToiDea:", ex)

        return 0

    # =====================================================================
    #  METER
    # =====================================================================

    def load_meter_config(self):
        if not os.path.exists(self.meter_path):
            print("Meter_schedule.json non trovato, uso valori di default.")
            return

        try:
            with open(self.meter_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            self.contatore_prelievo_w = data.get("contatore_prelievo_w", self.contatore_prelievo_w)
            self.contatore_immissione_w = data.get("contatore_immissione_w", self.contatore_immissione_w)
            print(f"Meter config: prelievo={self.contatore_prelievo_w}W, immissione={self.contatore_immissione_w}W")
        except Exception as ex:
            print("Errore lettura Meter_schedule.json:", ex)

    def meter_convert_1090_to_power(self, par1090: float) -> tuple[float, float]:
        v = par1090
        if v <= 5000:
            ratio = (5000 - v) / 5000.0
            p_prelievo = ratio * self.contatore_prelievo_w
            return p_prelievo, 0.0
        else:
            ratio = (v - 5000) / 5000.0
            p_immissione = ratio * self.contatore_immissione_w
            return 0.0, p_immissione

    def meter_tick(self):
        par1090 = self.values["1090"]
        p_pre, p_imm = self.meter_convert_1090_to_power(par1090)
        print(f"METER: 1090={par1090:.0f} -> P_prelievo={p_pre:.0f}W  P_immissione={p_imm:.0f}W")

    # =====================================================================
    #  SCHEDULE BATTERIA / SERVIZI
    # =====================================================================

    def load_schedule(self):
        if not os.path.exists(self.schedule_path):
            print("battery_schedule.json non trovato, uso valori di default.")
            return {"entries": []}

        try:
            with open(self.schedule_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            batt = data.get("battery", {})
            self.capacity_kwh = batt.get("capacity_kwh", self.capacity_kwh)
            emerg = batt.get("emergency", {})
            self.emergency_start_soc_dec = emerg.get("start_soc_dec", self.emergency_start_soc_dec)
            self.emergency_stop_soc_dec = data.get("stop_carica_emergenza_soc_dec", self.emergency_stop_soc_dec)

            guard = batt.get("guardrail_autoconsumo", {})
            self.guardrail_1101_min = guard.get("par_1101_min", self.guardrail_1101_min)

            # lettura modalità servizi (auto/manual)
            services = data.get("services", {})
            for name in self.service_modes.keys():
                mode = services.get(name, self.service_modes[name]).lower()
                if mode in ("auto", "manual"):
                    self.service_modes[name] = mode

            print(f"Battery config: capacity={self.capacity_kwh}kWh, "
                  f"emergenza start={self.emergency_start_soc_dec}, stop={self.emergency_stop_soc_dec}, "
                  f"guardrail_1101_min={self.guardrail_1101_min}")
            print(f"Service modes: {self.service_modes}")

            return data

        except Exception as ex:
            print("Errore lettura battery_schedule.json:", ex)
            return {"entries": []}

    def print_missing_energy(self):
        soc_dec = self.values["1040"]
        soc_percent = soc_dec / 10.0
        energia_attuale = self.capacity_kwh * soc_percent / 100.0
        energia_mancante = self.capacity_kwh - energia_attuale
        print(f"ENERGIA: SOC={soc_percent:.1f}%  E_batt={energia_attuale:.2f}kWh  "
              f"E_mancante={energia_mancante:.2f}kWh")

    # =====================================================================
    #  DSO / TRADING - SCRIPT ESTERNI
    # =====================================================================

    def _run_external_script(self, script_name: str):
        script_path = os.path.join(self.base_path, script_name)
        if not os.path.exists(script_path):
            print(f"Script {script_name} non trovato in {self.base_path}")
            return

        try:
            subprocess.Popen(["python", script_path])
            print(f"Lanciato script esterno: {script_name}")
        except Exception as ex:
            print(f"Errore nel lancio di {script_name}: {ex}")

    def carica_forzata_dso(self):
        self._run_external_script("carica_forzata_dso.py")

    def scarica_forzata_dso(self):
        self._run_external_script("scarica_forzata_dso.py")

    def partizione_batteria_scarica_trading(self):
        self._run_external_script("partizione_batteria_scarica_trading.py")

    def partizione_batteria_carica_trading(self):
        self._run_external_script("partizione_batteria_carica_trading.py")

    def auto_start_services(self):
        service_to_script = {
            "carica_forzata_dso": "carica_forzata_dso.py",
            "scarica_forzata_dso": "scarica_forzata_dso.py",
            "trading_scarica": "partizione_batteria_scarica_trading.py",
            "trading_carica": "partizione_batteria_carica_trading.py",
        }

        for name, mode in self.service_modes.items():
            if mode != "auto":
                continue
            if name in self.started_services:
                continue
            script = service_to_script.get(name)
            if script:
                self._run_external_script(script)
                self.started_services.add(name)

    # =====================================================================
    #  CICLO PRINCIPALE
    # =====================================================================

    def tick(self):
        print("\nTICK BATTERIA", time.strftime("%H:%M:%S"))

        if not self.read_from_idea():
            return

        # 1) Verifica se c'è un servizio DSO/Trading attivo (SERVICE=1)
        service_active = self.read_service_active()
        if service_active:
            print("Servizio DSO/Trading attivo: carica emergenza DISABILITATA.")
        else:
            # 2) Carica emergenza (safety) solo se NON ci sono servizi
            self.handle_emergency_charge()

        # 3) Energia mancante per 100%
        self.print_missing_energy()

        # 4) Meter ogni 30 secondi
        now = time.time()
        if now - self.last_meter_time >= 30.0:
            self.meter_tick()
            self.last_meter_time = now


if __name__ == "__main__":
    controller = BatteryController()

    try:
        while True:
            controller.tick()
            time.sleep(5)
    except KeyboardInterrupt:
        print("\nBatteryController terminato dall'utente.")
