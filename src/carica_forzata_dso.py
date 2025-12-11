import os
import time
import json
import xml.etree.ElementTree as ET
from datetime import datetime, date, time as dtime
from pathlib import Path


class CaricaForzataDSO:
    """
    Servizio DSO: POTENZA A SCENDERE (carica forzata).

    Fasi:

    1) PRE-DISCHARGE (prima dell'evento)
       - se SOC (1040) > 50 (5%):
         * AUTOCONSUMO=0 (stop controller_idea)
         * 1102 = 3 (batteria normale)
         * 1101 = 6000 (scarica massima)
       - quando SOC <= 50:
         * 1102 = 0 (batteria esclusa)
         * AUTOCONSUMO=1 (riprende autoconsumo locale)
         * attesa evento

    2) EVENTO (tra start e end programmati)
       - AUTOCONSUMO=0
       - 1102 = 1 (solo carica)
       - 1101 = -6000 (carica massima da rete)

    3) POST-EVENTO
       - 1102 = 3 (stato normale)
       - AUTOCONSUMO=1
       - SERVICE=0 (nessun servizio attivo)
       - script termina.
    """

    def __init__(self):
        # ====== PERCORSI ======
        self.base_path = Path.home() / "Desktop" / "alfa_test"
        self.from_path = os.path.join(self.base_path, "FromiDea.xml")
        self.to_path = os.path.join(self.base_path, "ToiDea.xml")
        self.guardrail_path = os.path.join(self.base_path, "guardrail_auto-consumo_locale.txt")
        self.schedule_path = os.path.join(self.base_path, "battery_schedule.json")
        # NUOVO: stato servizi globali
        self.service_status_path = os.path.join(self.base_path, "service_status.txt")

        # valori letti da FromiDea
        self.values = {"1040": 0.0, "1013": 0.0}

        # stato
        self.current_id = 0
        self.state = "INIT"  # INIT, PRE_DISCHARGE, WAIT_EVENT, EVENT_ACTIVE, DONE

        # carica programma DSO
        self.program = self.load_dso_program()
        if self.program is None:
            print("Nessun programma DSO 'carica_forzata_dso' trovato per oggi. Esco.")
            self.active = False
            return

        self.event_start, self.event_end = self.compute_event_times(self.program)
        if self.event_start is None or self.event_end is None:
            print("Date/ore programma DSO non valide. Esco.")
            self.active = False
            return

        print(f"Programma DSO trovato: {self.program['id']}")
        print(f"Evento dalle {self.event_start} alle {self.event_end}")

        # NUOVO: segna che un servizio DSO/Trading è attivo
        self.write_service_status(True)

        self.active = True

    # =====================================================================
    #  SUPPORTO: SERVICE_STATUS
    # =====================================================================

    def write_service_status(self, active: bool):
        """
        Scrive SERVICE=1/0 nel file service_status.txt in modo
        che battery_controller.py sappia se deve disattivare la carica emergenza.
        """
        try:
            with open(self.service_status_path, "w", encoding="utf-8") as f:
                f.write(f"SERVICE={'1' if active else '0'}")
            print(f"service_status -> SERVICE={'1' if active else '0'}")
        except Exception as ex:
            print("Errore scrittura service_status:", ex)

    # =====================================================================
    #  SUPPORTO: LETTURA SCHEDULE
    # =====================================================================

    def load_dso_program(self):
        if not os.path.exists(self.schedule_path):
            print("battery_schedule.json non trovato.")
            return None

        try:
            with open(self.schedule_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            today_str = date.today().isoformat()
            programs = data.get("dso_programs", [])

            for p in programs:
                if p.get("mode") != "carica_forzata_dso":
                    continue
                days = p.get("days", [])
                if today_str in days:
                    return p

            return None

        except Exception as ex:
            print("Errore lettura battery_schedule.json:", ex)
            return None

    def compute_event_times(self, program):
        try:
            today = date.today()
            start_str = program.get("start", "00:00")
            end_str = program.get("end", "00:15")

            h_s, m_s = map(int, start_str.split(":"))
            h_e, m_e = map(int, end_str.split(":"))

            start_dt = datetime.combine(today, dtime(hour=h_s, minute=m_s))
            end_dt = datetime.combine(today, dtime(hour=h_e, minute=m_e))

            return start_dt, end_dt

        except Exception as ex:
            print("Errore calcolo orari evento:", ex)
            return None, None

    # =====================================================================
    #  SUPPORTO: LETTURA FromiDea
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

    def read_from_idea(self) -> bool:
        try:
            if not os.path.exists(self.from_path):
                print("FromiDea.xml non trovato.")
                return False

            with open(self.from_path, "r", encoding="utf-8") as f:
                txt = f.read()

            self.values["1040"] = self._get_value_from_tag(txt, "1040")
            self.values["1013"] = self._get_value_from_tag(txt, "1013")

            soc = self.values["1040"]
            ibat = self.values["1013"]
            print(f"SOC(1040)={soc:.0f}  IBAT(1013)={ibat:.0f}")
            return True

        except Exception as ex:
            print("Errore lettura FromiDea:", ex)
            return False

    # =====================================================================
    #  SUPPORTO: ToiDea + guardrail
    # =====================================================================

    def update_toi_dea(self, ind_value: int, val_value: int) -> None:
        try:
            if not os.path.exists(self.to_path):
                print("ToiDea.xml non trovato.")
                return

            tree = ET.parse(self.to_path)
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

            tree.write(self.to_path, encoding="utf-8", xml_declaration=True)

            print(f"ToiDea -> ID={self.current_id}  IND={ind_value}  VAL={val_value}")

        except Exception as ex:
            print("Errore aggiornamento ToiDea:", ex)

    def write_guardrail_autoconsumo(self, enable: bool):
        try:
            with open(self.guardrail_path, "w", encoding="utf-8") as f:
                f.write(f"AUTOCONSUMO={'1' if enable else '0'}")
            print(f"Guardrail AUTOCONSUMO={'1' if enable else '0'}")
        except Exception as ex:
            print("Errore scrittura guardrail:", ex)

    # =====================================================================
    #  LOGICA DI STATO
    # =====================================================================

    def tick(self):
        if not self.active:
            return

        now = datetime.now()
        print(f"\n[TICK DSO] {now.strftime('%H:%M:%S')}  stato={self.state}")

        # se evento già finito → chiusura e DONE
        if now >= self.event_end and self.state != "DONE":
            print("Evento DSO terminato (ora oltre end). Fase post-evento.")
            self.finish_event()
            self.state = "DONE"
            return

        if not self.read_from_idea():
            return

        soc = self.values["1040"]

        # FASE 1: PRE-DISCHARGE (prima dell'evento)
        if now < self.event_start:
            if soc > 50:
                print("PRE-DISCHARGE: scarico batteria fino a SOC=5% (1040=50)")
                self.state = "PRE_DISCHARGE"
                self.write_guardrail_autoconsumo(False)
                self.update_toi_dea(1102, 3)
                self.update_toi_dea(1101, 6000)
            else:
                if self.state != "WAIT_EVENT":
                    print("PRE-DISCHARGE completata. Batteria scarica. Attesa evento.")
                    self.update_toi_dea(1102, 0)
                    self.write_guardrail_autoconsumo(True)
                    self.state = "WAIT_EVENT"
            return

        # FASE 2: EVENTO ATTIVO
        if self.event_start <= now < self.event_end:
            print("EVENTO DSO ATTIVO: carica forzata (potenza a scendere)")
            self.state = "EVENT_ACTIVE"
            self.write_guardrail_autoconsumo(False)
            self.update_toi_dea(1102, 1)
            self.update_toi_dea(1101, -6000)
            return

        # la fase 3 (post-evento) viene gestita da finish_event() quando now >= end

    def finish_event(self):
        print("Ripristino stato batteria post-DSO.")
        self.update_toi_dea(1102, 3)
        self.write_guardrail_autoconsumo(True)
        # NUOVO: segnala che non ci sono più servizi attivi
        self.write_service_status(False)
        print("Servizio carica_forzata_dso completato.")


# =====================================================================
#  MAIN
# =====================================================================

if __name__ == "__main__":
    dso = CaricaForzataDSO()

    if not getattr(dso, "active", False):
        # nessun programma trovato o errore iniziale
        # assicuriamoci comunque di rimettere SERVICE=0
        try:
            dso.write_service_status(False)
        except Exception:
            pass
        exit(0)

    try:
        while dso.state != "DONE":
            dso.tick()
            time.sleep(5)
    except KeyboardInterrupt:
        print("\ncarica_forzata_dso terminato dall'utente.")
    finally:
        # in ogni caso, a fine script, garantiamo SERVICE=0
        try:
            dso.write_service_status(False)
        except Exception:
            pass

