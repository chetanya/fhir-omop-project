# FHIR → OMOP Learning Project

A structured, hands-on learning environment for mastering FHIR R4 → OMOP CDM
transformation on a Databricks lakehouse, with focus on Indian ABDM profiles.

**Designed for:** Senior data engineers with Databricks/Delta Lake experience
who are new to clinical data standards.

---

## Quick Start

```bash
# 1. Clone this repo
git clone <your-repo-url>
cd fhir-omop-project

# 2. Generate synthetic FHIR data (Synthea)
git clone https://github.com/synthetichealth/synthea ../synthea
cd ../synthea
./run_synthea -p 1000 "Maharashtra" --exporter.fhir.export=true
cp -r output/fhir ../fhir-omop-project/data/synthea/fhir/
cd ../fhir-omop-project

# 3. Run unit tests locally (no cluster needed)
pip install pytest pyspark fhir.resources
pytest tests/ -v

# 4. Open in Claude Code
# Claude Code reads CLAUDE.md automatically — all context is pre-loaded
```

---

## Learning Path

| Phase | Steps | Focus |
|-------|-------|-------|
| 1 — Foundations | 1–3 | Synthea → Bronze → Silver |
| 2 — FHIR→OMOP Mapping | 4–10 | All core OMOP tables + Achilles DQD |
| 3 — India/ABDM Layer | 11–15 | ABHA IDs, consent, Indian drug/dx coding |
| 4 — RWD Research Layer | 16–19 | Cohorts, federated queries, RWE studies |

Ask Claude Code `"status"` at any time to see where you are.

---

## Key Open Source Dependencies

| Tool | Purpose | Link |
|------|---------|------|
| Synthea | Synthetic FHIR patient generator | github.com/synthetichealth/synthea |
| dbignite | FHIR parsing on Databricks | databricks-industry-solutions/dbignite |
| OMOP CDM Accelerator | Reference Delta Lake OMOP setup | databricks-industry-solutions/omop-cdm |
| FHIR→OMOP Cookbook | Mapping methodology reference | CodeX-HL7-FHIR-Accelerator/fhir2omop-cookbook |
| OHDSI/FhirToCdm | Official OHDSI FHIR→OMOP tool | github.com/OHDSI/FhirToCdm |
| OMOPonFHIR | Bidirectional FHIR↔OMOP server | omoponfhir.org |
| HAPI FHIR | FHIR server for local validation | hapifhir.io |
| OHDSI Athena | OMOP vocabulary download | athena.ohdsi.org |

---

## Using This Project with Claude (claude.ai)

1. Create a new **Claude Project** at claude.ai
2. Paste the contents of `CLAUDE_PROJECT_SYSTEM_PROMPT.md` into Project Instructions
3. Attach as Project files:
   - `docs/abdm_gap_analysis.md`
   - `docs/mapping_decisions.md`
   - ABDM FHIR IG v6.5 PDF (download from abdm.gov.in)
   - OMOP CDM v5.4 DDL (from OHDSI GitHub)

Then every conversation in that Project will have full context of your stack,
learning progress, and mapping decisions.

---

## Using This Project with Claude Code (terminal)

Claude Code reads `CLAUDE.md` automatically when you open this directory.
No additional setup needed — just run `claude` in this directory.

Useful prompts to get started:
- `"Generate the dbt model for FHIR Condition → OMOP condition_occurrence"`
- `"Write a Synthea config to generate diabetic patients in Maharashtra"`  
- `"What ABDM extensions do I need to handle in the Encounter mapping?"`
- `"Run the patient mapping tests and fix any failures"`
