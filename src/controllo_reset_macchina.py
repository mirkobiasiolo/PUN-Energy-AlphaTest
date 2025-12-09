import os
import time
import xml.etree.ElementTree as ET
from pathlib import Path


class MachineStateResetService:
    """
    Servizio di watchdog su stato macchina (par. 1070).

    Logica:
    - ogni 30 s legge 1070 da FromiDea.xml
    - se 1070 == 2  -> macchina OK, azzera contatore tentativi e (opz.) resetta allarme
    - se 1070 in (0, 1) -> prova a mandare un reset: scrive 1103 = 10 su ToiDea.xml
    - se dopo 5 tentativi 1070 è ancora 0/1 -> smette di mandare reset e crea messaggio "MACCHINA IN ALLARME"
    """

    def __init__(self):
        # ====== PERCORSI FILE ======
        self.base_path = Path.home() / "Desktop" / "test 2025-12-06"
        self.from_path = os.path.join(self.base_path, "FromiDea.xml")
        self.to_path = os.path.join(self.base_path, "ToiDea.xml")
        self.alarm_file_path = os.path.join(self.base_path, "MacchinaAllarme.txt")

        # stato interno
        self.current_id = 0
        self.reset_attempts = 0
        self.max_attempts = 5
        self.alarm_active = False

        # ultimo valore letto
        self.values = {"1070": 0.0}

    # =====================================================================
    #  UTILITIES: parsing FromiDea (tag numerici)
    # =====================================================================

    @staticmethod
    def _get_value_from_tag(xml_text: str, tag_name: str) -> float:
        """
        Estrae il valore tra <tagName>...</tagName> da una stringa di testo.
        Stessa utility usata negli altri script iDea.
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
    #  LETTURA FromiDea: stato macchina 1070
    # =====================================================================

    def read_state_1070(self) -> bool:
        """Legge 1070 da FromiDea.xml e aggiorna self.values['1070']."""
        try:
            if not os.path.exists(self.from_path):
                print("FromiDea.xml non trovato.")
                return False

            with open(self.from_path, "r", encoding="utf-8") as f:
                xml_text = f.read()

            self.values["1070"] = self._get_value_from_tag(xml_text, "1070")
            stato = int(self.values["1070"])
            print(f"[RESET] Stato macchina 1070={stato}")
            return True

        except Exception as ex:
            print("Errore lettura FromiDea:", ex)
            return False

    # =====================================================================
    #  SCRITTURA ToiDea: comando reset (1103 = 10)
    # =====================================================================

    def update_toi_dea(self, ind_value: int, val_value: int) -> None:
        """
        Aggiorna ID / IND / VAL in ToiDea.xml.
        CMD e DTYPE restano quelli già presenti nel file (es. CMD=07, DTYPE=U16).
        """
        try:
            if not os.path.exists(self.to_path):
                print("ToiDea.xml non trovato.")
                return

            tree = ET.parse(self.to_path)
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

            tree.write(self.to_path, encoding="utf-8", xml_declaration=True)

            print(f"[RESET] ToiDea -> ID={self.current_id}  IND={ind_value}  VAL={val_value}")

        except Exception as ex:
            print("Errore aggiornamento ToiDea:", ex)

    # =====================================================================
    #  MESSAGGIO ALLARME
    # =====================================================================

    def write_alarm_message(self, active: bool) -> None:
        """
        Scrive un piccolo file di stato.
        Se active=True  -> contiene 'MACCHINA IN ALLARME'
        Se active=False -> contiene 'MACCHINA OK'
        """
        try:
            with open(self.alarm_file_path, "w", encoding="utf-8") as f:
                if active:
                    f.write("MACCHINA IN ALLARME")
                else:
                    f.write("MACCHINA OK")
            print(f"[RESET] Alarm file aggiornato: active={active}")
        except Exception as ex:
            print("Errore scrittura file allarme:", ex)

    # =====================================================================
    #  TICK LOGICO (da chiamare ogni 30 secondi)
    # =====================================================================

    def tick(self) -> None:
        if not self.read_state_1070():
            return

        stato = int(self.values["1070"])

        # Caso macchina OK
        if stato == 2:
            if self.reset_attempts > 0 or self.alarm_active:
                print("[RESET] Macchina tornata operativa, azzero contatore tentativi/allarme.")
            self.reset_attempts = 0
            if self.alarm_active:
                self.alarm_active = False
                self.write_alarm_message(False)
            return

        # Caso macchina in sleep o errore (0/1)
        if stato in (0, 1):
            # Se siamo già in allarme, non facciamo più nulla
            if self.alarm_active:
                print("[RESET] Macchina ancora in allarme, nessun nuovo reset.")
                return

            if self.reset_attempts < self.max_attempts:
                self.reset_attempts += 1
                print(f"[RESET] Tentativo di reset #{self.reset_attempts} (1070={stato})")
                # Par. 1103 = 10 → richiesta reset errori
                self.update_toi_dea(1103, 10)
            else:
                # Raggiunto numero massimo di tentativi
                print(">>> MACCHINA IN ALLARME: superati i tentativi di reset <<<")
                self.alarm_active = True
                self.write_alarm_message(True)
            return

        # Altri stati (diversi da 0,1,2): per ora solo log
        print(f"[RESET] Stato 1070={stato} non gestito in modo speciale.")


def main():
    service = MachineStateResetService()
    ticket_time_s = 15

    print(f"Servizio controllo_reset stato macchina avviato (tick={ticket_time_s}s). Ctrl-C per uscire.")
    try:
        while True:
            service.tick()
            time.sleep(ticket_time_s)
    except KeyboardInterrupt:
        print("Uscita richiesta dall'utente.")


if __name__ == "__main__":
    main()
