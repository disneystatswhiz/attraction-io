# TODO Checklist: Attraction-IO Parallel Polling & Modelling Pipeline

- [ ] **Review and document pipeline flow for all property/type combinations**
- [ ] Ensure robust detection of new data files (standby and priority) for all properties
- [ ] Confirm parallel job launching works for all property/type slots
- [ ] Add/improve logging to capture:
    - [ ] File detection timestamps and status
    - [ ] Job start and completion times for each slot
    - [ ] Skipped jobs with clear reasons (e.g. “no forecasts available”)
- [ ] Add detailed debug logging for any entity/model skipped or not run
- [ ] Finalize/improve imputation logic for POSTED, ACTUAL, and PRIORITY types
- [ ] Refactor pipeline to support new wait time types (future proofing)
- [ ] Test for edge cases:
    - [ ] Late-arriving files
    - [ ] Missing or corrupted input files
    - [ ] Empty or all-missing entity data
- [ ] (Optional) Add notifications for job failures/completions (Slack, email, etc.)
- [ ] (Optional) Generate daily or per-run summary reports for ops review
- [ ] (Optional) Dashboard showing job status and recent run times
- [ ] Document pipeline logic and update README for current architecture

---

- [ ] Drop rows from modelling where geo_decay_wgt is 0 - maybe that will speed up modelling?
- [ ] Make custom descriptive summary for priority entities - to treat the 8888s
- [ ] Check why 8pm to 11pm hours dont have averages for MK44 (perhaps others as well) on descp summary
- [ ] Check why/if forecasts are not appending from day-to-day on S3

