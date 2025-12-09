import os
import time
import json
import xml.etree.ElementTree as ET
from datetime import datetime, date, time as dtime
from typing import Tuple

CONFIG_FILENAME = "config_idea_unificato.json"


# ======================================================================
#  UTIL / CONFIG MANAGER
# ======================================================================

def _get_value_from_tag(xml_text: str, tag_name: str) -> float:
    """Estrae il valore tra <tagName>...</tagName> da una stringa XML 'flat'."""
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


class ConfigManager:
    """
    Gestisce il JSON unificato e mantiene in sync i vecchi file
    (guardrail, service_status, DebitoEnergetico, MacchinaAllarme, sharingiDea).

    Da ora lo sharing/community è comandato SOLO da flags.sharing_enabled.
    sharingiDea.xml viene solo scritto come mirror.
    """

    def __init__(self, base_path: str | None = None):
        self.data = self._default_config()
        if base_path is not None:
            self.data["base_path"] = base_path

        self.base_path = self.data.get("base_path", os.getcwd())
        self.config_path = os.path.join(self.base_path, CONFIG_FILENAME)
        self._last_mtime: float = 0.0

        if os.path.exists(self.config_path):
            self._load()
        else:
            os.makedirs(self.base_path, exist_ok=True)
            self._save()

        self.sync_flag_files()

    # ------------------------------------------------------------------
    #  Default
    # ------------------------------------------------------------------
    def _default_config(self) -> dict:
        return {
            "base_path": ".",
            "paths": {
                "from_local": "FromiDea.xml",
                "from_remote": "FromiDea_remoto.xml",
                "toidea": "ToiDea.xml",
                "sharing_xml": "sharingiDea.xml",
                "debito_txt": "DebitoEnergetico.txt",
                "guardrail_txt": "guardrail_auto-consumo_locale.txt",
                "service_status_txt": "service_status.txt",
                "macchina_allarme_txt": "MacchinaAllarme.txt"
            },
            "meter": {
                "contatore_prelievo_w": 3000,
                "contatore_immissione_w": 3000
            },
            "battery": {
                "capacity_kwh": 8.0,
                "emergency": {
                    "start_soc_dec": 50,
                    "stop_soc_dec": 400,
                    "ibat_low_min": 0,
                    "ibat_low_max": 500,
                    "step_emergency_1101": 50,
                    "emergency_1101_max": 0,
                    "use_meter_control": True,
                    "grid_limit_w": 2800,
                    "grid_hysteresis_w": 100
                },
                "guardrail_autoconsumo": {
                    "par_1101_min": -2500
                }
            },
            "autoconsumo": {
                "param1101_min": 70,
                "param1101_max": 6000,
                "grid_setpoint_1090": 5000,
                "grid_deadband": 50,
                "step_local": 20,
                "step_community": 20,
                "bt_loss_factor": 0.10,
                "soc_threshold_community": 950
            },
            "services": {
                "carica_forzata_dso": "manual",
                "scarica_forzata_dso": "manual",
                "trading_scarica": "manual",
                "trading_carica": "manual"
            },
            "programs": {
                "dso_programs": [],
                "trading_programs": []
            },
            "flags": {
                "autoconsumo_enabled": True,
                "service_active": False,
                "macchina_allarme": False,
                "debito_energetico": False,
                "sharing_enabled": True
            }
        }

    # ------------------------------------------------------------------
    #  IO JSON
    # ------------------------------------------------------------------
    def _load(self) -> None:
        with open(self.config_path, "r", encoding="utf-8") as f:
            self.data = json.load(f)
        self.base_path = self.data.get("base_path", self.base_path)
        try:
            self._last_mtime = os.path.getmtime(self.config_path)
        except OSError:
            self._last_mtime = 0.0
        print("[CFG] JSON caricato.")

    def _save(self) -> None:
        os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
        with open(self.config_path, "w", encoding="utf-8") as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)
        try:
            self._last_mtime = os.path.getmtime(self.config_path)
        except OSError:
            self._last_mtime = 0.0

    def save(self) -> None:
        self._save()

    def reload_if_changed(self) -> None:
        """Ricarica il JSON se è stato modificato esternamente."""
        try:
            mtime = os.path.getmtime(self.config_path)
        except OSError:
            return
        if mtime != self._last_mtime:
            print("[CFG] Modifica esterna rilevata, ricarico JSON.")
            self._load()
            # dopo un reload riallineo anche i file legacy
            self.sync_flag_files()

    # ------------------------------------------------------------------
    #  PATH helper
    # ------------------------------------------------------------------
    def path(self, key: str) -> str:
        rel = self.data.get("paths", {}).get(key)
        if not rel:
            raise KeyError(f"Percorso '{key}' non definito nel JSON.")
        return os.path.join(self.base_path, rel)

        # ------------------------------------------------------------------
    #  FLAGS (con sync su file legacy)
    # ------------------------------------------------------------------
    def is_autoconsumo_enabled(self) -> bool:
        return bool(self.data.get("flags", {}).get("autoconsumo_enabled", True))

    def set_autoconsumo_enabled(self, enabled: bool) -> None:
        # aggiorno solo stato in RAM + file guardrail
        self.data.setdefault("flags", {})["autoconsumo_enabled"] = bool(enabled)
        self._write_guardrail_txt(enabled)
        # NIENTE self._save(): il JSON non viene riscritto in loop

    def is_service_active(self) -> bool:
        return bool(self.data.get("flags", {}).get("service_active", False))

    def set_service_active(self, active: bool) -> None:
        self.data.setdefault("flags", {})["service_active"] = bool(active)
        self._write_service_status_txt(active)
        # niente _save()

    def is_debito_energetico(self) -> bool:
        return bool(self.data.get("flags", {}).get("debito_energetico", False))

    def set_debito_energetico(self, active: bool) -> None:
        self.data.setdefault("flags", {})["debito_energetico"] = bool(active)
        self._write_debito_txt(active)
        # niente _save()

    def is_macchina_allarme(self) -> bool:
        return bool(self.data.get("flags", {}).get("macchina_allarme", False))

    def set_macchina_allarme(self, active: bool) -> None:
        self.data.setdefault("flags", {})["macchina_allarme"] = bool(active)
        self._write_macchina_allarme_txt(active)
        # niente _save()

    def is_sharing_enabled(self) -> bool:
        return bool(self.data.get("flags", {}).get("sharing_enabled", True))

    def set_sharing_enabled(self, enabled: bool) -> None:
        # per ora lo usiamo solo se un domani vuoi cambiarlo da codice,
        # ma lo lasciamo comunque "non persistente" sul JSON per evitare conflitti
        self.data.setdefault("flags", {})["sharing_enabled"] = bool(enabled)
        self._write_sharing_xml(enabled)
        # niente _save()


    # ------------------------------------------------------------------
    #  SYNC iniziale dei file legacy
    # ------------------------------------------------------------------
    def sync_flag_files(self) -> None:
        self._write_guardrail_txt(self.is_autoconsumo_enabled())
        self._write_service_status_txt(self.is_service_active())
        self._write_debito_txt(self.is_debito_energetico())
        self._write_macchina_allarme_txt(self.is_macchina_allarme())
        self._write_sharing_xml(self.is_sharing_enabled())

    # ------------------------------------------------------------------
    #  WRITER file legacy
    # ------------------------------------------------------------------
    def _write_guardrail_txt(self, enabled: bool) -> None:
        try:
            path = self.path("guardrail_txt")
            with open(path, "w", encoding="utf-8") as f:
                f.write(f"AUTOCONSUMO={'1' if enabled else '0'}")
            print(f"[CFG] guardrail -> AUTOCONSUMO={'1' if enabled else '0'}")
        except Exception as ex:
            print("Errore scrittura guardrail_txt:", ex)

    def _write_service_status_txt(self, active: bool) -> None:
        try:
            path = self.path("service_status_txt")
            with open(path, "w", encoding="utf-8") as f:
                f.write(f"SERVICE={'1' if active else '0'}")
            print(f"[CFG] service_status -> SERVICE={'1' if active else '0'}")
        except Exception as ex:
            print("Errore scrittura service_status_txt:", ex)

    def _write_debito_txt(self, active: bool) -> None:
        try:
            path = self.path("debito_txt")
            with open(path, "w", encoding="utf-8") as f:
                f.write(f"DebitoEnergetico={'1' if active else '0'}")
            print(f"[CFG] DebitoEnergetico={'1' if active else '0'}")
        except Exception as ex:
            print("Errore scrittura DebitoEnergetico.txt:", ex)

    def _write_macchina_allarme_txt(self, active: bool) -> None:
        try:
            path = self.path("macchina_allarme_txt")
            with open(path, "w", encoding="utf-8") as f:
                if active:
                    f.write("MACCHINA IN ALLARME")
                else:
                    f.write("MACCHINA OK")
            print(f"[CFG] MacchinaAllarme={'IN ALLARME' if active else 'OK'}")
        except Exception as ex:
            print("Errore scrittura MacchinaAllarme.txt:", ex)

    def _write_sharing_xml(self, enabled: bool) -> None:
        try:
            path = self.path("sharing_xml")
            root = ET.Element("rootmain")
            ET.SubElement(root, "DATA").text = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            ET.SubElement(root, "sharing").text = "1" if enabled else "0"
            tree = ET.ElementTree(root)
            tree.write(path, encoding="utf-8", xml_declaration=True)
            print(f"[CFG] sharingiDea -> sharing={'1' if enabled else '0'}")
        except Exception as ex:
            print("Errore scrittura sharingiDea.xml:", ex)


# ======================================================================
#  TOIDEA MANAGER (ID unico + cache registri 110x)
# ======================================================================

class ToiDeaManager:
    def __init__(self, cfg: ConfigManager):
        self.cfg = cfg
        self.current_id = 0
        self.last_values: dict[int, int] = {}

    def write_register(self, reg: int, val: int) -> None:
        """Aggiorna un registro nel ToiDea.xml (IND/VAL) e incrementa ID."""
        path = self.cfg.path("toidea")
        if not os.path.exists(path):
            print("ToiDea.xml non trovato.")
            return

        try:
            tree = ET.parse(path)
            root = tree.getroot()

            self.current_id += 1
            if self.current_id >= 6000:
                self.current_id = 0

            id_node = root.find(".//ID")
            if id_node is not None:
                id_node.text = str(self.current_id)

            ind_node = root.find(".//IND")
            if ind_node is not None:
                ind_node.text = str(reg)

            val_node = root.find(".//VAL")
            if val_node is not None:
                val_node.text = str(int(val))

            tree.write(path, encoding="utf-8", xml_declaration=True)

            self.last_values[reg] = int(val)
            print(f"[ToiDea] ID={self.current_id} IND={reg} VAL={val}")
        except Exception as ex:
            print("Errore aggiornamento ToiDea.xml:", ex)

    def get_register(self, reg: int, default: int = 0) -> int:
        """Ritorna l'ultimo valore scritto per un registro 110x (cache locale)."""
        return int(self.last_values.get(reg, default))


# ======================================================================
#  CONTROLLER AUTOCONSUMO / COMMUNITY
# ======================================================================

class IdeaNodeControllerUnified:
    def __init__(self, cfg: ConfigManager, toi: ToiDeaManager):
        self.cfg = cfg
        self.toi = toi

        acfg = self.cfg.data.get("autoconsumo", {})
        self.current_setpoint_1101 = acfg.get("param1101_min", 70)
        self.param1101_min = acfg.get("param1101_min", 70)
        self.param1101_max = acfg.get("param1101_max", 6000)
        self.grid_setpoint_1090 = acfg.get("grid_setpoint_1090", 5000)
        self.grid_deadband = acfg.get("grid_deadband", 50)
        self.step_local = acfg.get("step_local", 20)
        self.step_community = acfg.get("step_community", 20)
        self.bt_loss_factor = acfg.get("bt_loss_factor", 0.1)
        self.soc_threshold_community = acfg.get("soc_threshold_community", 950)

        self.values = {
            "1040": 0.0,
            "1070": 0.0,
            "1090": 0.0,
            "1060": 0.0
        }

    def _read_local_fromidea(self) -> bool:
        path = self.cfg.path("from_local")
        if not os.path.exists(path):
            print("[AUTOCONS] FromiDea locale non trovato.")
            return False
        try:
            with open(path, "r", encoding="utf-8") as f:
                xml_text = f.read()
            for k in self.values.keys():
                self.values[k] = _get_value_from_tag(xml_text, k)
            print(f"[AUTOCONS] 1090={self.values['1090']:.0f}  "
                  f"1040={self.values['1040']:.0f}  1070={self.values['1070']:.0f}")
            return True
        except Exception as ex:
            print("Errore lettura FromiDea (autoconsumo):", ex)
            return False

    def _read_remote_1090(self) -> float:
        path = self.cfg.path("from_remote")
        if not os.path.exists(path):
            return 0.0
        try:
            with open(path, "r", encoding="utf-8") as f:
                xml_text = f.read()
            return _get_value_from_tag(xml_text, "1090")
        except Exception as ex:
            print("Errore lettura FromiDea_remoto:", ex)
            return 0.0

    def _regolazione_autoconsumo_locale(self, sensor1090: float) -> None:
        misura = int(sensor1090)
        if misura < 0 or misura > 10000:
            return

        errore = self.grid_setpoint_1090 - misura

        if abs(errore) <= self.grid_deadband:
            pass
        elif errore > 0:
            self.current_setpoint_1101 += self.step_local
        else:
            self.current_setpoint_1101 -= self.step_local

        if self.current_setpoint_1101 < self.param1101_min:
            self.current_setpoint_1101 = self.param1101_min
        if self.current_setpoint_1101 > self.param1101_max:
            self.current_setpoint_1101 = self.param1101_max

        if (self.current_setpoint_1101 >= self.param1101_max and
                misura < (self.grid_setpoint_1090 - self.grid_deadband)):
            self.cfg.set_debito_energetico(True)
        else:
            self.cfg.set_debito_energetico(False)

        self.toi.write_register(1101, int(self.current_setpoint_1101))

    def _balance_energy(self, local1090: float, remote1090: float) -> None:
        misura_local = int(local1090)
        misura_remote = int(remote1090)

        if misura_local < 0 or misura_local > 10000:
            return
        if misura_remote < 0 or misura_remote > 10000:
            return
        if misura_remote >= self.grid_setpoint_1090:
            return

        diff_remote = self.grid_setpoint_1090 - misura_remote
        if diff_remote <= 0:
            return

        target_local = (self.grid_setpoint_1090 +
                        diff_remote +
                        diff_remote * self.bt_loss_factor)

        errore_local = target_local - misura_local

        if abs(errore_local) <= self.grid_deadband:
            return
        elif errore_local > 0:
            self.current_setpoint_1101 += self.step_community
        else:
            self.current_setpoint_1101 -= self.step_community

        if self.current_setpoint_1101 > self.param1101_max:
            self.current_setpoint_1101 = self.param1101_max
        if self.current_setpoint_1101 < self.param1101_min:
            self.current_setpoint_1101 = self.param1101_min

        self.toi.write_register(1101, int(self.current_setpoint_1101))

    def tick(self) -> None:
        print("\n[TICK AUTOCONS]", time.strftime("%H:%M:%S"))

        if not self.cfg.is_autoconsumo_enabled():
            print("[AUTOCONS] Autoconsumo/Community PAUSATI (batteria / servizi).")
            return

        if not self._read_local_fromidea():
            return

        misura1090 = self.values["1090"]
        soc1040 = self.values["1040"]

        if misura1090 < 0 or misura1090 > 10000:
            return

        debito = self.cfg.is_debito_energetico()
        sharing_ok = self.cfg.is_sharing_enabled()

        remote1090 = 0.0
        if sharing_ok:
            remote1090 = self._read_remote_1090()

        cond_debito = not debito
        cond_soc = soc1040 >= self.soc_threshold_community
        cond_sharing = sharing_ok
        cond_remote_deficit = (remote1090 > 0 and
                               remote1090 < self.grid_setpoint_1090)
        cond_locale_ok = misura1090 >= (self.grid_setpoint_1090 -
                                        2 * self.grid_deadband)

        sharing_active = (cond_debito and cond_soc and cond_sharing and
                          cond_remote_deficit and cond_locale_ok)

        if sharing_active:
            print("[AUTOCONS] Modalità COMMUNITY attiva")
            self._balance_energy(misura1090, remote1090)
        else:
            print("[AUTOCONS] Modalità AUTOCONSUMO LOCALE")
            self._regolazione_autoconsumo_locale(misura1090)


# ======================================================================
#  CONTROLLER BATTERIA / EMERGENZA / METER
# ======================================================================

class BatteryControllerUnified:
    """
    Gestisce:
    - carica di emergenza,
    - meter interno (1090 -> W),
    - energia mancante in kWh.

    Carica emergenza:
    - entra quando SOC= start_soc_dec e corrente batteria in [ibat_low_min, ibat_low_max];
    - mette 1102=1 (solo carica) e ferma l'autoconsumo;
    - usa il METER per rendere 1101 NEGATIVO in modo da non superare grid_limit_w
      sul prelievo totale di casa (contatore interno).
    """

    def __init__(self, cfg: ConfigManager, toi: ToiDeaManager):
        self.cfg = cfg
        self.toi = toi

        # Meter prima (serve per default grid_limit_w)
        mcfg = self.cfg.data.get("meter", {})
        self.contatore_prelievo_w = mcfg.get("contatore_prelievo_w", 6000)
        self.contatore_immissione_w = mcfg.get("contatore_immissione_w", 6000)

        bcfg = self.cfg.data.get("battery", {})
        emerg = bcfg.get("emergency", {})
        guard = bcfg.get("guardrail_autoconsumo", {})

        self.capacity_kwh = bcfg.get("capacity_kwh", 10.0)
        self.emergency_start_soc_dec = emerg.get("start_soc_dec", 50)
        self.emergency_stop_soc_dec = emerg.get("stop_soc_dec", 400)

        self.guardrail_1101_min = guard.get("par_1101_min", -2500)

        self.ibat_low_min = emerg.get("ibat_low_min", 0)
        self.ibat_low_max = emerg.get("ibat_low_max", 500)

        # per safety, se vuoi ancora limitare corrente batteria
        self.ibat_min = emerg.get("ibat_min", 5000)
        self.ibat_max = emerg.get("ibat_max", 6000)

        self.step_emergency_1101 = emerg.get("step_emergency_1101", 50)
        self.emergency_1101_max = emerg.get("emergency_1101_max", 0)

        self.emergency_use_meter = emerg.get("use_meter_control", True)
        self.emergency_grid_limit_w = emerg.get("grid_limit_w", self.contatore_prelievo_w)
        self.emergency_grid_hysteresis_w = emerg.get("grid_hysteresis_w", 100)

        self.emergency_active = False
        self.values = {
            "1040": 0.0,
            "1013": 0.0,
            "1090": 0.0
        }

        self.last_meter_time = 0.0

    def _read_fromidea(self) -> bool:
        path = self.cfg.path("from_local")
        if not os.path.exists(path):
            print("[BATT] FromiDea.xml non trovato.")
            return False
        try:
            with open(path, "r", encoding="utf-8") as f:
                xml_text = f.read()
            for k in self.values.keys():
                self.values[k] = _get_value_from_tag(xml_text, k)
            print(f"[BATT] SOC(1040)={self.values['1040']:.0f}  "
                  f"IBAT(1013)={self.values['1013']:.0f}  "
                  f"1090={self.values['1090']:.0f}")
            return True
        except Exception as ex:
            print("Errore lettura FromiDea (batteria):", ex)
            return False

    def _handle_emergency_charge(self) -> None:
        soc = self.values["1040"]
        ibat = self.values["1013"]
        par1090 = self.values["1090"]

        # Ingresso in emergenza
        if (not self.emergency_active and
                soc == self.emergency_start_soc_dec and
                self.ibat_low_min <= ibat <= self.ibat_low_max):
            print(">>> [BATT] INGRESSO CARICA EMERGENZA <<<")
            self.emergency_active = True
            self.toi.write_register(1102, 1)  # solo carica
            self.cfg.set_autoconsumo_enabled(False)

        if not self.emergency_active:
            return

        # Uscita
        if soc >= self.emergency_stop_soc_dec:
            print(">>> [BATT] USCITA CARICA EMERGENZA <<<")
            self.emergency_active = False
            self.toi.write_register(1102, 3)  # carica+scarica
            self.cfg.set_autoconsumo_enabled(True)
            self.toi.write_register(1101, 0)
            return

        # --- Controllo tramite METER (prelievo limitato) ---
        if not self.emergency_use_meter:
            print("[BATT] Carica emergenza: use_meter_control=False (nessuna logica avanzata).")
            return

        p_pre, p_imm = self._meter_convert_1090_to_power(par1090)
        limit = float(self.emergency_grid_limit_w)
        hyst = float(self.emergency_grid_hysteresis_w)
        current_1101 = self.toi.get_register(1101, 0)

        if p_pre < (limit - hyst):
            # possiamo caricare di più dalla rete -> 1101 più NEGATIVO
            nuovo_1101 = current_1101 - self.step_emergency_1101
        elif p_pre > (limit + hyst):
            # stiamo superando il limite -> riduco carica (meno negativo, verso 0)
            nuovo_1101 = current_1101 + self.step_emergency_1101
        else:
            print(f"[BATT] Carica emergenza: p_pre={p_pre:.0f}W vicino al limite={limit:.0f}W, 1101 invariato.")
            return

        # Limiti numerici: non superare par_1101_min in negativo, e mai > emergency_1101_max (0)
        if nuovo_1101 < self.guardrail_1101_min:
            nuovo_1101 = self.guardrail_1101_min
        if nuovo_1101 > self.emergency_1101_max:
            nuovo_1101 = self.emergency_1101_max

        # Guardrail anche su corrente batteria (se necessario)
        if ibat > self.ibat_max:
            print("[BATT] Ibatt > ibat_max, riduco ulteriormente la carica.")
            nuovo_1101 = max(nuovo_1101 + self.step_emergency_1101, self.emergency_1101_max)

        self.toi.write_register(1101, int(nuovo_1101))
        print(f"[BATT] Carica emergenza: p_pre={p_pre:.0f}W, "
              f"limite={limit:.0f}W, 1101={nuovo_1101}W")

    def _print_missing_energy(self) -> None:
        soc_dec = self.values["1040"]
        soc_percent = soc_dec / 10.0
        energia_attuale = self.capacity_kwh * soc_percent / 100.0
        energia_mancante = self.capacity_kwh - energia_attuale
        print(f"[BATT] ENERGIA: SOC={soc_percent:.1f}%  "
              f"E_batt={energia_attuale:.2f}kWh  "
              f"E_mancante={energia_mancante:.2f}kWh")

    def _meter_convert_1090_to_power(self, par1090: float) -> Tuple[float, float]:
        v = par1090
        if v <= 5000:
            ratio = (5000 - v) / 5000.0
            p_prelievo = ratio * self.contatore_prelievo_w
            return p_prelievo, 0.0
        else:
            ratio = (v - 5000) / 5000.0
            p_immissione = ratio * self.contatore_immissione_w
            return 0.0, p_immissione

    def _meter_tick(self) -> None:
        par1090 = self.values["1090"]
        p_pre, p_imm = self._meter_convert_1090_to_power(par1090)
        print(f"[METER] 1090={par1090:.0f} -> "
              f"P_prelievo={p_pre:.0f}W  P_immissione={p_imm:.0f}W")

    def tick(self) -> None:
        print("\n[TICK BATT]", time.strftime("%H:%M:%S"))

        if not self._read_fromidea():
            return

        if self.cfg.is_service_active():
            print("[BATT] Servizio DSO/Trading attivo: carica emergenza DISABILITATA.")
        else:
            self._handle_emergency_charge()

        self._print_missing_energy()

        now = time.time()
        if now - self.last_meter_time >= 1.0:
            self._meter_tick()
            self.last_meter_time = now


# ======================================================================
#  DSO / TRADING (classi come in versione precedente)
#  - CaricaForzataDSOUnified
#  - ScaricaForzataDSOUnified
#  - TradingScaricaUnified
#  - TradingCaricaUnified
# ======================================================================

# (Per brevità non commento ogni riga, la logica è invariata rispetto
#  alla versione unificata precedente, solo integrata con il nuovo
#  ConfigManager.)

class CaricaForzataDSOUnified:
    def __init__(self, cfg: ConfigManager, toi: ToiDeaManager):
        self.cfg = cfg
        self.toi = toi
        self.values = {"1040": 0.0, "1013": 0.0}
        self.state = "INIT"
        self.active = False

        mode = cfg.data.get("services", {}).get("carica_forzata_dso", "manual").lower()
        if mode != "auto":
            print("[DSO CARICA] carica_forzata_dso='manual': non attivo.")
            return

        self.program = self._load_dso_program_for_today()
        if self.program is None:
            print("[DSO CARICA] Nessun programma per oggi.")
            return

        self.event_start, self.event_end = self._compute_event_times(self.program)
        if self.event_start is None or self.event_end is None:
            print("[DSO CARICA] Orari non validi.")
            return

        print(f"[DSO CARICA] Programma: {self.program.get('id')}")
        print(f"[DSO CARICA] Evento {self.event_start} -> {self.event_end}")

        self.cfg.set_service_active(True)
        self.active = True

    def _load_dso_program_for_today(self):
        progs = self.cfg.data.get("programs", {}).get("dso_programs", [])
        today_str = date.today().isoformat()
        for p in progs:
            if p.get("mode") != "carica_forzata_dso":
                continue
            if today_str in p.get("days", []):
                return p
        return None

    def _compute_event_times(self, program):
        try:
            today = date.today()
            start_str = program.get("start", "00:00")
            end_str = program.get("end", "00:15")
            h_s, m_s = map(int, start_str.split(":"))
            h_e, m_e = map(int, end_str.split(":"))
            return (
                datetime.combine(today, dtime(hour=h_s, minute=m_s)),
                datetime.combine(today, dtime(hour=h_e, minute=m_e)),
            )
        except Exception as ex:
            print("Errore orari DSO carica:", ex)
            return None, None

    def _read_fromidea(self) -> bool:
        path = self.cfg.path("from_local")
        if not os.path.exists(path):
            print("[DSO CARICA] FromiDea.xml non trovato.")
            return False
        try:
            with open(path, "r", encoding="utf-8") as f:
                txt = f.read()
            self.values["1040"] = _get_value_from_tag(txt, "1040")
            self.values["1013"] = _get_value_from_tag(txt, "1013")
            print(f"[DSO CARICA] SOC={self.values['1040']:.0f}  IBAT={self.values['1013']:.0f}")
            return True
        except Exception as ex:
            print("Errore FromiDea (DSO carica):", ex)
            return False

    def _write_guardrail_autoconsumo(self, enable: bool) -> None:
        self.cfg.set_autoconsumo_enabled(enable)

    def _finish_event(self) -> None:
        print("[DSO CARICA] Ripristino post-DSO.")
        self.toi.write_register(1102, 3)
        self._write_guardrail_autoconsumo(True)
        self.cfg.set_service_active(False)
        print("[DSO CARICA] Fine servizio.")

    def ensure_service_flag_cleared(self) -> None:
        self.cfg.set_service_active(False)

    def tick(self) -> None:
        if not self.active:
            return

        now = datetime.now()
        print(f"\n[TICK DSO CARICA] {now.strftime('%H:%M:%S')} stato={self.state}")

        if now >= self.event_end and self.state != "DONE":
            print("[DSO CARICA] Evento concluso.")
            self._finish_event()
            self.state = "DONE"
            return

        if not self._read_fromidea():
            return

        soc = self.values["1040"]

        if now < self.event_start:
            if soc > 50:
                print("[DSO CARICA] PRE-DISCHARGE (scarica fino a 5%).")
                self.state = "PRE_DISCHARGE"
                self._write_guardrail_autoconsumo(False)
                self.toi.write_register(1102, 3)
                self.toi.write_register(1101, 6000)
            else:
                if self.state != "WAIT_EVENT":
                    print("[DSO CARICA] PRE-DISCHARGE completata, attesa evento.")
                    self.toi.write_register(1102, 0)
                    self._write_guardrail_autoconsumo(True)
                    self.state = "WAIT_EVENT"
            return

        if self.event_start <= now < self.event_end:
            print("[DSO CARICA] EVENTO ATTIVO: carica forzata.")
            self.state = "EVENT_ACTIVE"
            self._write_guardrail_autoconsumo(False)
            self.toi.write_register(1102, 1)
            self.toi.write_register(1101, -6000)
            return


class ScaricaForzataDSOUnified:
    def __init__(self, cfg: ConfigManager, toi: ToiDeaManager):
        self.cfg = cfg
        self.toi = toi
        self.values = {"1040": 0.0, "1013": 0.0}
        self.state = "INIT"
        self.active = False

        mode = cfg.data.get("services", {}).get("scarica_forzata_dso", "manual").lower()
        if mode != "auto":
            print("[DSO SCARICA] scarica_forzata_dso='manual': non attivo.")
            return

        self.program = self._load_dso_program_for_today()
        if self.program is None:
            print("[DSO SCARICA] Nessun programma per oggi.")
            return

        self.event_start, self.event_end = self._compute_event_times(self.program)
        if self.event_start is None or self.event_end is None:
            print("[DSO SCARICA] Orari non validi.")
            return

        acfg = self.cfg.data.get("autoconsumo", {})
        self.target_soc_high_dec = acfg.get("soc_threshold_community", 950)
        bcfg = self.cfg.data.get("battery", {})
        emerg = bcfg.get("emergency", {})
        self.min_soc_dec = emerg.get("start_soc_dec", 50)

        print(f"[DSO SCARICA] Programma: {self.program.get('id')}")
        print(f"[DSO SCARICA] Evento {self.event_start} -> {self.event_end}")
        print(f"[DSO SCARICA] Target_precharge={self.target_soc_high_dec}  SOC_min={self.min_soc_dec}")

        self.cfg.set_service_active(True)
        self.active = True

    def _load_dso_program_for_today(self):
        progs = self.cfg.data.get("programs", {}).get("dso_programs", [])
        today_str = date.today().isoformat()
        for p in progs:
            if p.get("mode") != "scarica_forzata_dso":
                continue
            if today_str in p.get("days", []):
                return p
        return None

    def _compute_event_times(self, program):
        try:
            today = date.today()
            start_str = program.get("start", "00:00")
            end_str = program.get("end", "00:15")
            h_s, m_s = map(int, start_str.split(":"))
            h_e, m_e = map(int, end_str.split(":"))
            return (
                datetime.combine(today, dtime(hour=h_s, minute=m_s)),
                datetime.combine(today, dtime(hour=h_e, minute=m_e)),
            )
        except Exception as ex:
            print("Errore orari DSO scarica:", ex)
            return None, None

    def _read_fromidea(self) -> bool:
        path = self.cfg.path("from_local")
        if not os.path.exists(path):
            print("[DSO SCARICA] FromiDea.xml non trovato.")
            return False
        try:
            with open(path, "r", encoding="utf-8") as f:
                txt = f.read()
            self.values["1040"] = _get_value_from_tag(txt, "1040")
            self.values["1013"] = _get_value_from_tag(txt, "1013")
            print(f"[DSO SCARICA] SOC={self.values['1040']:.0f}  IBAT={self.values['1013']:.0f}")
            return True
        except Exception as ex:
            print("Errore FromiDea (DSO scarica):", ex)
            return False

    def _write_guardrail_autoconsumo(self, enable: bool) -> None:
        self.cfg.set_autoconsumo_enabled(enable)

    def _finish_event(self) -> None:
        print("[DSO SCARICA] Ripristino post-DSO.")
        self.toi.write_register(1102, 3)
        self.toi.write_register(1101, 0)
        self._write_guardrail_autoconsumo(True)
        self.cfg.set_service_active(False)
        print("[DSO SCARICA] Fine servizio.")

    def ensure_service_flag_cleared(self) -> None:
        self.cfg.set_service_active(False)

    def tick(self) -> None:
        if not self.active:
            return

        now = datetime.now()
        print(f"\n[TICK DSO SCARICA] {now.strftime('%H:%M:%S')} stato={self.state}")

        if now >= self.event_end and self.state != "DONE":
            print("[DSO SCARICA] Evento concluso.")
            self._finish_event()
            self.state = "DONE"
            return

        if not self._read_fromidea():
            return

        soc = self.values["1040"]

        if now < self.event_start:
            if soc < self.target_soc_high_dec:
                print("[DSO SCARICA] PRE-CHARGE: carico verso soglia alta.")
                self.state = "PRE_CHARGE"
                self._write_guardrail_autoconsumo(False)
                self.toi.write_register(1102, 1)
                self.toi.write_register(1101, -6000)
            else:
                if self.state != "WAIT_EVENT":
                    print("[DSO SCARICA] PRE-CHARGE completata, attesa evento.")
                    self.toi.write_register(1102, 0)
                    self._write_guardrail_autoconsumo(True)
                    self.state = "WAIT_EVENT"
            return

        if self.event_start <= now < self.event_end:
            self.state = "EVENT_ACTIVE"
            self._write_guardrail_autoconsumo(False)
            if soc > self.min_soc_dec:
                print("[DSO SCARICA] EVENTO ATTIVO: scarica forzata.")
                self.toi.write_register(1102, 3)
                self.toi.write_register(1101, 6000)
            else:
                print("[DSO SCARICA] SOC sotto soglia minima, fermo scarica.")
                self.toi.write_register(1101, 0)
            return


class TradingScaricaUnified:
    def __init__(self, cfg: ConfigManager, toi: ToiDeaManager):
        self.cfg = cfg
        self.toi = toi
        self.values = {"1040": 0.0}
        self.state = "INIT"
        self.active = False

        mode = cfg.data.get("services", {}).get("trading_scarica", "manual").lower()
        if mode != "auto":
            print("[TRADING SCARICA] trading_scarica='manual': non attivo.")
            return

        self.program = self._load_trading_program_for_today()
        if self.program is None:
            print("[TRADING SCARICA] Nessun programma per oggi.")
            return

        self.event_start, self.event_end = self._compute_event_times(self.program)
        if self.event_start is None or self.event_end is None:
            print("[TRADING SCARICA] Orari non validi.")
            return

        self.target_min_soc_dec = int(self.program.get("partition_soc_dec", 300))
        print(f"[TRADING SCARICA] Programma: {self.program.get('id')}")
        print(f"[TRADING SCARICA] Evento {self.event_start} -> {self.event_end}")
        print(f"[TRADING SCARICA] SOC minimo target={self.target_min_soc_dec}")

        self.cfg.set_service_active(True)
        self.active = True

    def _load_trading_program_for_today(self):
        progs = self.cfg.data.get("programs", {}).get("trading_programs", [])
        today_str = date.today().isoformat()
        for p in progs:
            if p.get("mode") != "partizione_scarica_trading":
                continue
            if today_str in p.get("days", []):
                return p
        return None

    def _compute_event_times(self, program):
        try:
            today = date.today()
            start_str = program.get("start", "00:00")
            end_str = program.get("end", "00:30")
            h_s, m_s = map(int, start_str.split(":"))
            h_e, m_e = map(int, end_str.split(":"))
            return (
                datetime.combine(today, dtime(hour=h_s, minute=m_s)),
                datetime.combine(today, dtime(hour=h_e, minute=m_e)),
            )
        except Exception as ex:
            print("Errore orari Trading scarica:", ex)
            return None, None

    def _read_fromidea(self) -> bool:
        path = self.cfg.path("from_local")
        if not os.path.exists(path):
            print("[TRADING SCARICA] FromiDea.xml non trovato.")
            return False
        try:
            with open(path, "r", encoding="utf-8") as f:
                txt = f.read()
            self.values["1040"] = _get_value_from_tag(txt, "1040")
            print(f"[TRADING SCARICA] SOC={self.values['1040']:.0f}")
            return True
        except Exception as ex:
            print("Errore FromiDea (Trading scarica):", ex)
            return False

    def _write_guardrail_autoconsumo(self, enable: bool) -> None:
        self.cfg.set_autoconsumo_enabled(enable)

    def _finish_event(self) -> None:
        print("[TRADING SCARICA] Ripristino post-trading.")
        self.toi.write_register(1102, 3)
        self.toi.write_register(1101, 0)
        self._write_guardrail_autoconsumo(True)
        self.cfg.set_service_active(False)
        print("[TRADING SCARICA] Fine servizio.")

    def ensure_service_flag_cleared(self) -> None:
        self.cfg.set_service_active(False)

    def tick(self) -> None:
        if not self.active:
            return

        now = datetime.now()
        print(f"\n[TICK TRADING SCARICA] {now.strftime('%H:%M:%S')} stato={self.state}")

        if now >= self.event_end and self.state != "DONE":
            print("[TRADING SCARICA] Evento concluso.")
            self._finish_event()
            self.state = "DONE"
            return

        if not self._read_fromidea():
            return

        soc = self.values["1040"]

        if now < self.event_start:
            print("[TRADING SCARICA] In attesa inizio evento.")
            self.state = "WAIT_EVENT"
            return

        if self.event_start <= now < self.event_end:
            self.state = "EVENT_ACTIVE"
            self._write_guardrail_autoconsumo(False)
            if soc > self.target_min_soc_dec:
                print("[TRADING SCARICA] EVENTO ATTIVO: scarica trading.")
                self.toi.write_register(1102, 3)
                self.toi.write_register(1101, 6000)
            else:
                print("[TRADING SCARICA] SOC <= target, fermo scarica.")
                self.toi.write_register(1101, 0)
            return


class TradingCaricaUnified:
    def __init__(self, cfg: ConfigManager, toi: ToiDeaManager):
        self.cfg = cfg
        self.toi = toi
        self.values = {"1040": 0.0}
        self.state = "INIT"
        self.active = False

        mode = cfg.data.get("services", {}).get("trading_carica", "manual").lower()
        if mode != "auto":
            print("[TRADING CARICA] trading_carica='manual': non attivo.")
            return

        self.program = self._load_trading_program_for_today()
        if self.program is None:
            print("[TRADING CARICA] Nessun programma per oggi.")
            return

        self.event_start, self.event_end = self._compute_event_times(self.program)
        if self.event_start is None or self.event_end is None:
            print("[TRADING CARICA] Orari non validi.")
            return

        acfg = self.cfg.data.get("autoconsumo", {})
        default_target = acfg.get("soc_threshold_community", 950)
        self.target_soc_high_dec = int(self.program.get("partition_soc_dec", default_target))

        print(f"[TRADING CARICA] Programma: {self.program.get('id')}")
        print(f"[TRADING CARICA] Evento {self.event_start} -> {self.event_end}")
        print(f"[TRADING CARICA] SOC alto target={self.target_soc_high_dec}")

        self.cfg.set_service_active(True)
        self.active = True

    def _load_trading_program_for_today(self):
        progs = self.cfg.data.get("programs", {}).get("trading_programs", [])
        today_str = date.today().isoformat()
        for p in progs:
            if p.get("mode") != "partizione_carica_trading":
                continue
            if today_str in p.get("days", []):
                return p
        return None

    def _compute_event_times(self, program):
        try:
            today = date.today()
            start_str = program.get("start", "00:00")
            end_str = program.get("end", "00:30")
            h_s, m_s = map(int, start_str.split(":"))
            h_e, m_e = map(int, end_str.split(":"))
            return (
                datetime.combine(today, dtime(hour=h_s, minute=m_s)),
                datetime.combine(today, dtime(hour=h_e, minute=m_e)),
            )
        except Exception as ex:
            print("Errore orari Trading carica:", ex)
            return None, None

    def _read_fromidea(self) -> bool:
        path = self.cfg.path("from_local")
        if not os.path.exists(path):
            print("[TRADING CARICA] FromiDea.xml non trovato.")
            return False
        try:
            with open(path, "r", encoding="utf-8") as f:
                txt = f.read()
            self.values["1040"] = _get_value_from_tag(txt, "1040")
            print(f"[TRADING CARICA] SOC={self.values['1040']:.0f}")
            return True
        except Exception as ex:
            print("Errore FromiDea (Trading carica):", ex)
            return False

    def _write_guardrail_autoconsumo(self, enable: bool) -> None:
        self.cfg.set_autoconsumo_enabled(enable)

    def _finish_event(self) -> None:
        print("[TRADING CARICA] Ripristino post-trading carica.")
        self.toi.write_register(1102, 3)
        self.toi.write_register(1101, 0)
        self._write_guardrail_autoconsumo(True)
        self.cfg.set_service_active(False)
        print("[TRADING CARICA] Fine servizio.")

    def ensure_service_flag_cleared(self) -> None:
        self.cfg.set_service_active(False)

    def tick(self) -> None:
        if not self.active:
            return

        now = datetime.now()
        print(f"\n[TICK TRADING CARICA] {now.strftime('%H:%M:%S')} stato={self.state}")

        if now >= self.event_end and self.state != "DONE":
            print("[TRADING CARICA] Evento concluso.")
            self._finish_event()
            self.state = "DONE"
            return

        if not self._read_fromidea():
            return

        soc = self.values["1040"]

        if now < self.event_start:
            print("[TRADING CARICA] In attesa inizio evento.")
            self.state = "WAIT_EVENT"
            return

        if self.event_start <= now < self.event_end:
            self.state = "EVENT_ACTIVE"
            if soc < self.target_soc_high_dec:
                print("[TRADING CARICA] EVENTO ATTIVO: carica trading.")
                self._write_guardrail_autoconsumo(False)
                self.toi.write_register(1102, 1)
                self.toi.write_register(1101, -6000)
            else:
                print("[TRADING CARICA] SOC >= target, fermo carica.")
                self.toi.write_register(1101, 0)
            return


# ======================================================================
#  WATCHDOG STATO MACCHINA / RESET ERRORI
# ======================================================================

class MachineStateResetUnified:
    def __init__(self, cfg: ConfigManager, toi: ToiDeaManager):
        self.cfg = cfg
        self.toi = toi
        self.reset_attempts = 0
        self.max_attempts = 5
        self.alarm_active = False
        self.values = {"1070": 0.0}

    def _read_state_1070(self) -> bool:
        path = self.cfg.path("from_local")
        if not os.path.exists(path):
            print("[RESET] FromiDea.xml non trovato.")
            return False
        try:
            with open(path, "r", encoding="utf-8") as f:
                xml_text = f.read()
            self.values["1070"] = _get_value_from_tag(xml_text, "1070")
            stato = int(self.values["1070"])
            print(f"[RESET] Stato macchina 1070={stato}")
            return True
        except Exception as ex:
            print("Errore lettura FromiDea (reset):", ex)
            return False

    def _write_alarm_message(self, active: bool) -> None:
        self.cfg.set_macchina_allarme(active)

    def tick(self) -> None:
        if not self._read_state_1070():
            return

        stato = int(self.values["1070"])

        if stato == 2:
            if self.reset_attempts > 0 or self.alarm_active:
                print("[RESET] Macchina operativa, azzero tentativi/allarme.")
            self.reset_attempts = 0
            if self.alarm_active:
                self.alarm_active = False
                self._write_alarm_message(False)
            return

        if stato in (0, 1):
            if self.alarm_active:
                print("[RESET] Macchina ancora in allarme, nessun nuovo reset.")
                return

            if self.reset_attempts < self.max_attempts:
                self.reset_attempts += 1
                print(f"[RESET] Tentativo di reset #{self.reset_attempts} (1070={stato})")
                self.toi.write_register(1103, 10)  # comando reset errori
            else:
                print(">>> [RESET] MACCHINA IN ALLARME: superati i tentativi <<<")
                self.alarm_active = True
                self._write_alarm_message(True)
            return

        print(f"[RESET] Stato 1070={stato} non gestito in modo speciale.")


# ======================================================================
#  APP UNIFICATA: SCHEDULER
# ======================================================================

class IdeaUnifiedApp:
    def __init__(self, base_path: str | None = None):
        self.cfg = ConfigManager(base_path)
        self.toi = ToiDeaManager(self.cfg)

        self.autocons = IdeaNodeControllerUnified(self.cfg, self.toi)
        self.batt = BatteryControllerUnified(self.cfg, self.toi)
        self.reset = MachineStateResetUnified(self.cfg, self.toi)

        self.dso_carica = CaricaForzataDSOUnified(self.cfg, self.toi)
        self.dso_scarica = ScaricaForzataDSOUnified(self.cfg, self.toi)
        self.trading_scarica = TradingScaricaUnified(self.cfg, self.toi)
        self.trading_carica = TradingCaricaUnified(self.cfg, self.toi)

    def run_forever(self) -> None:
        print("Controller iDea unificato avviato. Ctrl-C per uscire.")

        intervals = {
            "cfg_reload": 1.0,
            "autocons": 0.5,
            "batt": 5.0,
            "reset": 30.0,
            "dso_carica": 5.0,
            "dso_scarica": 5.0,
            "trading_scarica": 5.0,
            "trading_carica": 5.0,
        }
        next_run = {k: time.time() for k in intervals}

        try:
            while True:
                now = time.time()

                if now >= next_run["cfg_reload"]:
                    self.cfg.reload_if_changed()
                    next_run["cfg_reload"] = now + intervals["cfg_reload"]

                if now >= next_run["autocons"]:
                    self.autocons.tick()
                    next_run["autocons"] = now + intervals["autocons"]

                if now >= next_run["batt"]:
                    self.batt.tick()
                    next_run["batt"] = now + intervals["batt"]

                if now >= next_run["reset"]:
                    self.reset.tick()
                    next_run["reset"] = now + intervals["reset"]

                if getattr(self.dso_carica, "active", False) and now >= next_run["dso_carica"]:
                    self.dso_carica.tick()
                    next_run["dso_carica"] = now + intervals["dso_carica"]

                if getattr(self.dso_scarica, "active", False) and now >= next_run["dso_scarica"]:
                    self.dso_scarica.tick()
                    next_run["dso_scarica"] = now + intervals["dso_scarica"]

                if getattr(self.trading_scarica, "active", False) and now >= next_run["trading_scarica"]:
                    self.trading_scarica.tick()
                    next_run["trading_scarica"] = now + intervals["trading_scarica"]

                if getattr(self.trading_carica, "active", False) and now >= next_run["trading_carica"]:
                    self.trading_carica.tick()
                    next_run["trading_carica"] = now + intervals["trading_carica"]

                time.sleep(0.1)
        except KeyboardInterrupt:
            print("\nUscita richiesta dall'utente.")
        finally:
            self.cfg.set_service_active(False)


if __name__ == "__main__":
    app = IdeaUnifiedApp(base_path=None)
    app.run_forever()

