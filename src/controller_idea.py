import os
import time
import xml.etree.ElementTree as ET
from pathlib import Path


class IdeaNodeController:
    def __init__(self):
        # ====== PERCORSI FILE ======
        BASE_DIR  = Path.home() / "Desktop" / "alfa_test"
        
        self.local_fromiDea_path = BASE_DIR / "FromiDea.xml"
        self.remote_fromiDea_path = BASE_DIR / "FromiDea_remoto.xml"
        self.toiDea_path = BASE_DIR / "ToiDea.xml"
        self.energy_debt_file_path = BASE_DIR / "DebitoEnergetico.txt"
        self.sharing_file_path = BASE_DIR / "sharingiDea.xml"
        # NUOVO: guardrail per stop/ripartenza autoconsumo (batteria/DSO)
        self.guardrail_path = BASE_DIR / "guardrail_auto-consumo_locale.txt"

        # ====== PARAMETRI CONTROLLO LOCALE / COMMUNITY ======

        # Setpoint di potenza da/per rete (registro 1101) in modalità locale
        self.current_setpoint_1101 = 70
        self.param1101_max = 6000        # max 6 kW
        self.param1101_min = 70          # min per non far addormentare l'inverter

        # Target del sensore 1090 (0–10000 mV), 5000 = zero scambio col contatore
        self.grid_setpoint_1090 = 5000

        # Banda morta attorno al setpoint (mV)
        self.grid_deadband = 50          # puoi ritoccarla

        # Passi di regolazione
        self.step_local = 5              # regolazione autoconsumo
        self.step_community = 5          # regolazione cessione comunitaria

        # Fattore di perdita BT per la condivisione (es. 0.1 = +10%)
        self.bt_loss_factor = 0.10

        # Soglia SOC (1040) per abilitare la condivisione (dec%)
        # 950 = 95% come da protocollo
        self.soc_threshold_community = 950

        # Stato logico
        self.energy_debt = False         # True se non riusciamo a raggiungere 5000
        self.sharing_active = False      # True se stiamo cedendo energia alla community

        # Dizionario con gli ultimi valori letti da FromiDea
        self.values = {
            "1001": 0.0,
            "1002": 0.0,
            "1003": 0.0,
            "1004": 0.0,
            "1005": 0.0,
            "1010": 0.0,
            "1011": 0.0,
            "1012": 0.0,
            "1013": 0.0,
            "1040": 0.0,
            "1041": 0.0,
            "1042": 0.0,
            "1060": 0.0,
            "1070": 0.0,
            "1090": 0.0,
        }

        # Per il campo ID di ToiDea
        self.current_id = 0

        # Prima lettura e inizializzazione debito energetico
        self.read_local_from_idea()
        self.write_energy_debt(False)

    # =====================================================================
    #  UTILITIES
    # =====================================================================

    @staticmethod
    def _get_value_from_tag(xml_text: str, tag_name: str) -> float:
        """
        Estrae il valore tra <tagName>...</tagName> da una stringa di testo.
        Usato per FromiDea / FromiDea_remoto dove i tag sono numerici (non XML valido).
        """
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
    #  LETTURA FILE FromiDea / FromiDea_remoto
    # =====================================================================

    def read_local_from_idea(self) -> bool:
        """Legge il FromiDea locale e popola self.values."""
        try:
            if not os.path.exists(self.local_fromiDea_path):
                print("FromiDea locale non trovato.")
                return False

            with open(self.local_fromiDea_path, "r", encoding="utf-8") as f:
                xml_text = f.read()

            for key in self.values.keys():
                self.values[key] = self._get_value_from_tag(xml_text, key)

            # Simulazione TextBox: stampa i 3 valori chiave
            print(
                f"1090={self.values['1090']:.0f}  "
                f"1070={self.values['1070']:.0f}  "
                f"1060={self.values['1060']:.0f}"
            )

            return True

        except Exception as ex:
            print("Errore durante la lettura di FromiDea (testo):", ex)
            return False

    def read_remote_1090(self) -> float:
        """Legge solo il registro 1090 dal FromiDea remoto."""
        try:
            if not os.path.exists(self.remote_fromiDea_path):
                return 0.0

            with open(self.remote_fromiDea_path, "r", encoding="utf-8") as f:
                xml_text = f.read()

            return self._get_value_from_tag(xml_text, "1090")

        except Exception as ex:
            print("Errore durante la lettura del FromiDea remoto:", ex)
            return 0.0

    # =====================================================================
    #  FILE DebitoEnergetico
    # =====================================================================

    def write_energy_debt(self, debito: bool) -> None:
        """Scrive DebitoEnergetico.txt nel formato DebitoEnergetico=0/1."""
        try:
            content = "DebitoEnergetico=1" if debito else "DebitoEnergetico=0"
            with open(self.energy_debt_file_path, "w", encoding="utf-8") as f:
                f.write(content)
        except Exception as ex:
            print("Errore scrittura DebitoEnergetico:", ex)

    def read_energy_debt(self) -> bool:
        """Ritorna True se DebitoEnergetico.txt termina con =1."""
        try:
            if not os.path.exists(self.energy_debt_file_path):
                return False

            with open(self.energy_debt_file_path, "r", encoding="utf-8") as f:
                s = f.read().strip().upper()

            return s.endswith("=1")
        except Exception as ex:
            print("Errore lettura DebitoEnergetico:", ex)
            return False

    # =====================================================================
    #  FILE sharingiDea.xml
    # =====================================================================

    def read_sharing_flag(self) -> bool:
        """Ritorna True se sharingiDea.xml contiene <sharing>1</sharing>."""
        try:
            if not os.path.exists(self.sharing_file_path):
                return False

            tree = ET.parse(self.sharing_file_path)
            root = tree.getroot()
            node = root.find(".//sharing")
            if node is None:
                return False

            v = int(node.text.strip())
            return v == 1

        except Exception as ex:
            print("Errore lettura sharingiDea.xml:", ex)
            return False

    # =====================================================================
    #  GUARDRAIL AUTOCONSUMO (STOP/RIPRESA PER BATTERIA/DSO)
    # =====================================================================

    def read_guardrail_autoconsumo(self) -> bool:
        """
        Ritorna True se AUTOCONSUMO=1 o file assente (autoconsumo abilitato),
        False se AUTOCONSUMO=0 (autoconsumo da fermare).
        """
        try:
            if not os.path.exists(self.guardrail_path):
                return True
            with open(self.guardrail_path, "r", encoding="utf-8") as f:
                s = f.read().strip().upper()
            return s.endswith("=1")
        except Exception:
            return True

    # =====================================================================
    #  SCRITTURA ToiDea.xml
    # =====================================================================

    def update_toi_dea(self, ind_value: int, val_value: int) -> None:
        """Aggiorna ID, IND e VAL nel ToiDea.xml."""
        try:
            if not os.path.exists(self.toiDea_path):
                print("ToiDea.xml non trovato.")
                return

            tree = ET.parse(self.toiDea_path)
            root = tree.getroot()

            # ID ciclico 0..5999
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
            print("Errore durante l'aggiornamento di ToiDea:", ex)

    # =====================================================================
    #  LOGICA AUTOCONSUMO LOCALE
    # =====================================================================

    def regolazione_autoconsumo_locale(self, sensor1090: float) -> None:
        """Mantiene 1090 ~ 5000 regolando il registro 1101 (current_setpoint_1101)."""
        misura = int(sensor1090)

        # Sicurezza
        if misura < 0 or misura > 10000:
            return

        errore = self.grid_setpoint_1090 - misura

        # Banda morta: se siamo vicini a 5000 non muoviamo niente
        if abs(errore) <= self.grid_deadband:
            pass
        elif errore > 0:
            # misura < 5000 → stiamo prelevando dalla rete → aumento 1101
            self.current_setpoint_1101 += self.step_local
        else:
            # misura > 5000 → stiamo immettendo → riduco 1101
            self.current_setpoint_1101 -= self.step_local

        # Limiti
        if self.current_setpoint_1101 < self.param1101_min:
            self.current_setpoint_1101 = self.param1101_min
        if self.current_setpoint_1101 > self.param1101_max:
            self.current_setpoint_1101 = self.param1101_max

        # Debito energetico:
        # se siamo al massimo ma il sensore è ancora ben sotto il target → mancano Watt disponibili
        if (
            self.current_setpoint_1101 >= self.param1101_max
            and misura < (self.grid_setpoint_1090 - self.grid_deadband)
        ):
            self.energy_debt = True
        else:
            self.energy_debt = False

        self.write_energy_debt(self.energy_debt)

        # Scrive 1101 nel ToiDea
        self.update_toi_dea(1101, self.current_setpoint_1101)

    # =====================================================================
    #  LOGICA COMMUNITY (CESSIONE ENERGIA DINAMICA)
    # =====================================================================

    def balance_energy(self, local1090: float, remote1090: float) -> None:
        """
        Cessione energia verso il vicino B.

        A (locale) legge:
          - local1090  = proprio sensore 0–10V
          - remote1090 = sensore 0–10V di B

        Se B è sotto i 5000 (remote1090 < 5000), A si sposta su un target dinamico:

            diff = 5000 - remote1090
            target_local = 5000 + diff + diff * bt_loss_factor

        E poi regola 1101 per portare local1090 verso target_local.
        Se diff diminuisce (B si avvicina a 5000), target_local scende e
        A riduce 1101.
        """
        misura_local = int(local1090)
        misura_remote = int(remote1090)

        # Sicurezza sui range
        if misura_local < 0 or misura_local > 10000:
            return
        if misura_remote < 0 or misura_remote > 10000:
            return

        # Se il vicino NON è in deficit (>=5000) non facciamo nulla
        if misura_remote >= self.grid_setpoint_1090:
            return

        # Quanto manca al vicino per arrivare a 5000
        diff_remote = self.grid_setpoint_1090 - misura_remote
        if diff_remote <= 0:
            return

        # Target locale dinamico con offset di perdita BT
        target_local = (
            self.grid_setpoint_1090
            + diff_remote
            + diff_remote * self.bt_loss_factor
        )

        # Errore sul nostro 1090 rispetto al target di condivisione
        errore_local = target_local - misura_local

        # Se siamo già vicini al target, non cambiamo 1101
        if abs(errore_local) <= self.grid_deadband:
            return
        elif errore_local > 0:
            # local1090 < target → aumentiamo 1101
            self.current_setpoint_1101 += self.step_community
        else:
            # local1090 > target → riduciamo 1101
            self.current_setpoint_1101 -= self.step_community

        # Limiti fisici su 1101
        if self.current_setpoint_1101 > self.param1101_max:
            self.current_setpoint_1101 = self.param1101_max
        if self.current_setpoint_1101 < self.param1101_min:
            self.current_setpoint_1101 = self.param1101_min

        # Scrivo il nuovo 1101 in ToiDea.xml
        self.update_toi_dea(1101, self.current_setpoint_1101)

    # =====================================================================
    #  CICLO PRINCIPALE (equivalente mainTimer_Tick)
    # =====================================================================

    def tick(self) -> None:
        print("\nTICK", time.strftime("%H:%M:%S"))

        # NUOVO: se la batteria/DSO ha chiesto di fermare autoconsumo/community, esco.
        if not self.read_guardrail_autoconsumo():
            print("Autoconsumo/Community PAUSATI (carica batteria / DSO)")
            return

        # 1) Legge il FromiDea locale
        if not self.read_local_from_idea():
            return

        misura1090 = self.values["1090"]   # sensore 0–10V
        soc1040 = self.values["1040"]      # SOC batteria (dec%)

        # Misura fuori range → non facciamo nulla
        if misura1090 < 0 or misura1090 > 10000:
            return

        # 2) Legge DebitoEnergetico.txt
        debito_da_file = self.read_energy_debt()

        # 3) Legge eventuale consenso alla condivisione
        sharing_ok = self.read_sharing_flag()

        # 4) Se c'è possibile condivisione, leggo anche il FromiDea remoto
        remote1090 = 0.0
        if sharing_ok:
            remote1090 = self.read_remote_1090()

        # --- CONDIZIONI PER SHARING ATTIVO ---

        # a) non devo avere debito energetico locale
        cond_debito = not debito_da_file

        # b) SOC batteria >= soglia (es. 950 = 95%)
        cond_soc = soc1040 >= self.soc_threshold_community

        # c) sharingiDea = 1
        cond_sharing = sharing_ok

        # d) vicino in deficit (remote1090 < 5000)
        cond_remote_deficit = (remote1090 > 0 and remote1090 < self.grid_setpoint_1090)

        # e) noi almeno in autoconsumo (non forte prelievo):
        #    1090 >= 5000 - 2*banda, es. >= 4900
        cond_locale_ok = misura1090 >= (self.grid_setpoint_1090 - 2 * self.grid_deadband)

        self.sharing_active = (
            cond_debito
            and cond_soc
            and cond_sharing
            and cond_remote_deficit
            and cond_locale_ok
        )

        if self.sharing_active:
            print("Modalità COMMUNITY attiva")
            self.balance_energy(misura1090, remote1090)
        else:
            print("Modalità AUTOCONSUMO LOCALE")
            self.regolazione_autoconsumo_locale(misura1090)


# =====================================================================
#  MAIN LOOP
# =====================================================================

if __name__ == "__main__":
    controller = IdeaNodeController()

    try:
        while True:
            controller.tick()
            time.sleep(0.5)   # equivalente al timer da 0.5 secondi
    except KeyboardInterrupt:
        print("\nTerminato dall'utente.")
