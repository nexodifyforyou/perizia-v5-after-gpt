"""Synthetic page fixtures for PDF-quality tests (general, not a real perizia)."""

# A clean document: all 6 key sections present, money on >=2 pages, consistent
# internal pagination labels, plenty of text -> expected PDF_QUALITY_OK.
GOOD_PAGES = [
    {
        "page_number": 1,
        "text": (
            "TRIBUNALE DI MILANO - LOTTO 1. Identificazione dei beni e oggetto "
            "della vendita: descrizione del bene immobile con ampi locali e "
            "pertinenze esterne. Stato di possesso: immobile occupato dal "
            "debitore esecutato senza titolo opponibile alla procedura. "
            "Prezzo base indicato negli atti della procedura esecutiva. "
            "Pagina 1 di 2."
        ),
    },
    {
        "page_number": 2,
        "text": (
            "Vincoli e oneri: risultano iscritte ipoteca e trascritto "
            "pignoramento con relative formalita. Giudizio di conformita "
            "edilizia, catastale e urbanistica con verifica degli impianti e "
            "dell'agibilita. Valutazione e stima: prezzo base d'asta "
            "EUR 120.000,00 quale valore di mercato per la vendita giudiziaria. "
            "Costi e spese di regolarizzazione e deprezzamento sono stimati in "
            "EUR 8.500,00. Pagina 2 di 2."
        ),
    },
]

# Like GOOD_PAGES but with the 'possesso' section stripped -> exactly one key
# section missing -> expected PDF_QUALITY_WARNING (SOME_KEY_SECTIONS_WEAK).
WARNING_PAGES = [
    {
        "page_number": 1,
        "text": (
            "TRIBUNALE DI MILANO - LOTTO 1. Identificazione dei beni e oggetto "
            "della vendita: descrizione del bene immobile con ampi locali e "
            "pertinenze esterne riportate in perizia tecnica estimativa. "
            "Prezzo base indicato negli atti della procedura esecutiva. "
            "Pagina 1 di 2."
        ),
    },
    {
        "page_number": 2,
        "text": (
            "Vincoli e oneri: risultano iscritte ipoteca e trascritto "
            "pignoramento con relative formalita. Giudizio di conformita "
            "edilizia, catastale e urbanistica con verifica degli impianti e "
            "dell'agibilita. Valutazione e stima: prezzo base d'asta "
            "EUR 120.000,00 quale valore di mercato per la vendita giudiziaria. "
            "Costi e spese di regolarizzazione e deprezzamento sono stimati in "
            "EUR 8.500,00. Pagina 2 di 2."
        ),
    },
]

# Empty extracted text on every page -> DOCUMENT_TEXT_EMPTY.
EMPTY_PAGES = [
    {"page_number": 1, "text": ""},
    {"page_number": 2, "text": ""},
    {"page_number": 3, "text": ""},
]

# Mostly unreadable: 3 of 4 pages blank, one with content -> TOO_MANY_UNREADABLE_PAGES.
MANY_UNREADABLE_PAGES = [
    {"page_number": 1, "text": ""},
    {"page_number": 2, "text": ""},
    {
        "page_number": 3,
        "text": (
            "Identificazione dei beni e oggetto della vendita con descrizione "
            "del bene e relative pertinenze. Valutazione e stima del prezzo "
            "base d'asta pari a EUR 90.000,00 quale valore di mercato."
        ),
    },
    {"page_number": 4, "text": ""},
]

# Readable text but only lotto + possesso sections present (4 sections missing,
# including valuation and costi) -> KEY_SECTIONS_UNREADABLE block.
MISSING_KEY_SECTIONS_PAGES = [
    {
        "page_number": 1,
        "text": (
            "Identificazione dei beni e oggetto della vendita: descrizione del "
            "bene immobile con vani e pertinenze. Stato di possesso: immobile "
            "risulta occupato dal conduttore in forza di un contratto di "
            "locazione regolarmente registrato presso l'ufficio competente."
        ),
    },
    {
        "page_number": 2,
        "text": (
            "Ulteriore descrizione del bene e dei vani interni con indicazione "
            "delle superfici lorde e delle pertinenze esclusive annesse al "
            "lotto in oggetto della presente relazione tecnica descrittiva."
        ),
    },
]
