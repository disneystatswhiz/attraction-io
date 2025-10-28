# ===================================================================================== #
# ------------------------------ Attraction IO Runner --------------------------------- #
# ===================================================================================== #

include("../src/main_setup.jl")   # ensures Main.DATA_FACT
include("../src/main_runner.jl")  # ensures run_entity()

# full list of codes
CODES = [
    # ==== Disney California Adventure (CA) ====
    "CA03", "CA09", "CA109", "CA110", "CA148", "CA155", "CA167", "CA180",
    "CA188", "CA22", "CA28", "CA30", "CA39", "CA56", "CA67", "CA68",
    # ==== Disneyland (DL) ====
    "DL01", "DL02", "DL03", "DL07", "DL16", "DL179", "DL18", "DL180", "DL24", "DL27",
    "DL28", "DL33", "DL34", "DL37", "DL38", "DL39", "DL40", "DL42", "DL45", "DL46",
    "DL50", "DL53",
    # ==== Islands of Adventure (IA) ====
    "IA01", "IA06", "IA07", "IA08", "IA09", "IA13", "IA14", "IA15", "IA16", "IA65", "IA69",
    # ==== Universal Studios Florida (UF) ====
    "UF02", "UF06", "UF07", "UF12", "UF30", "UF48", "UF62", "UF63",
    # ==== Epic Universe (EU) ====
    "EU01", "EU04", "EU05", "EU06", "EU07", "EU09", "EU10", "EU11",
    # ==== Animal Kingdom (AK) ====
    "AK07", "AK11", "AK18", "AK85", "AK86",
    # ==== EPCOT (EP) ====
    "EP02", "EP04", "EP07", "EP09", "EP13", "EP14", "EP155", "EP16", "EP186", "EP20",
    # ==== Hollywood Studios (HS) ====
    "HS103", "HS104", "HS111", "HS112", "HS113", "HS12", "HS15", "HS20", "HS22",
    # ==== Magic Kingdom (MK) ====
    "MK01", "MK05", "MK06", "MK13", "MK141", "MK142", "MK15", "MK16", "MK23",
    "MK27", "MK28", "MK29", "MK34", "MK39", "MK43"
]

# -------------------------------------------------------------------------------------
# Sequential calls (safer than threads, no shared global conflicts)
# -------------------------------------------------------------------------------------
for code in CODES
    try
        run_entity(code; data_fact = Main.DATA_FACT)
    catch err
        @warn "run_entity failed" code exception=(err, catch_backtrace())
        cleanup_folders(code)
    end
end
