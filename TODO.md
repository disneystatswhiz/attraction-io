# TODO Checklist: Attraction-IO Parallel Polling & Modelling Pipeline

- [ ] **Review and document pipeline flow for all property/type combinations**
- [X] Ensure robust detection of new data files (standby and priority) for all properties
- [X] Confirm parallel job launching works for all property/type slots
- [X] Add/improve logging to capture:
    - [X] File detection timestamps and status
    - [X] Job start and completion times for each slot
    - [X] Skipped jobs with clear reasons (e.g. “no forecasts available”)
- [X] Add detailed debug logging for any entity/model skipped or not run
- [ ] Finalize/improve imputation logic for POSTED, ACTUAL, and PRIORITY types
- [ ] Refactor pipeline to support new wait time types (future proofing)
- [ ] Test for edge cases:
    - [ ] Late-arriving files
    - [ ] Missing or corrupted input files
    - [ ] Empty or all-missing entity data
- [ ] (Optional) Add notifications for job failures/completions (Slack, email, etc.)
- [ ] (Optional) Generate daily or per-run summary reports for ops review
- [ ] (Optional) Dashboard showing job status and recent run times
- [X] Document pipeline logic and update README for current architecture

---

- [X] Drop rows from modelling where geo_decay_wgt is 0 - maybe that will speed up modelling?
- [X] Make custom descriptive summary for priority entities - to treat the 8888s
- [ ] Check why 8pm to 11pm hours dont have averages for MK44 (perhaps others as well) on descp summary
- [X] - SOLVED Check why/if forecasts are not appending from day-to-day on S3
- [ ] Add/fix code so that full modelling doesn't re-run with same input data

