# ABDM FHIR Profiles vs Base R4 — Gap Analysis
# docs/abdm_gap_analysis.md
# Auto-populate this as you work through Phase 3

## Overview

ABDM FHIR profiles are extensions of base HL7 FHIR R4, defined in the
ABDM FHIR Implementation Guide v6.5. This document tracks every divergence
relevant to the FHIR → OMOP pipeline.

**Reference:** https://nrces.in/ndhm/fhir/r4 (ABDM FHIR IG)
**ABDM Sandbox:** https://sandbox.abdm.gov.in/docs

---

## 1. Patient Resource Differences

| Field | Base R4 | ABDM R4 | OMOP Impact |
|-------|---------|---------|-------------|
| `identifier[].system` | Any URI | Must include `https://healthid.ndhm.gov.in` for ABHA | Use as `person_source_value` |
| `identifier[].type` | Optional | ABHA ID type is coded | Extract type for identifier routing |
| Race extension | US Core `us-core-race` | Absent / not required | Default `race_concept_id = 0` |
| Religion | Not standard | ABDM adds religion extension | No OMOP mapping; store in note |
| Address | Standard | Pincode mandatory; district added | Map postal_code; district → location |

**ABDM ABHA ID system URLs:**
```
https://healthid.ndhm.gov.in        ← ABHA (Ayushman Bharat Health Account) ID
https://healthid.ndhm.gov.in/phr    ← ABHA PHR Address
https://facility.ndhm.gov.in        ← Health Facility Registry (HFR)
https://doctor.ndhm.gov.in          ← Healthcare Professional Registry (HPR)
```

---

## 2. Encounter Resource Differences

| Field | Base R4 | ABDM R4 | OMOP Impact |
|-------|---------|---------|-------------|
| `class` | ActEncounterCode | Uses ABDM-specific codes (AMB, IMP, EMER) | Map to `visit_type_concept_id` |
| `serviceProvider` | Organization ref | HFR facility code | → `care_site_id` |
| `participant` | Provider ref | HPR doctor code | → `provider_id` |
| Episode of care | Optional | Linked to ABDM episode | Track longitudinal care |

---

## 3. Consent Resource (ABDM-Specific)

ABDM adds a `Consent` resource not meaningfully present in typical Synthea data.
This is critical for the federated data platform use case.

```json
{
  "resourceType": "Consent",
  "meta": {
    "profile": ["https://nrces.in/ndhm/fhir/r4/StructureDefinition/Consent"]
  },
  "status": "active",
  "scope": {
    "coding": [{
      "system": "https://nrces.in/ndhm/fhir/r4/CodeSystem/ndhm-consent-scope",
      "code": "HIPS"   // Health Information Provider System
    }]
  },
  "patient": {"reference": "Patient/..."},
  "dateTime": "2024-01-01T00:00:00+05:30",
  "organization": [{"reference": "Organization/..."}],
  "policy": [{"uri": "https://ndhm.gov.in/privacy-policy"}]
}
```

**OMOP handling:** No direct OMOP CDM table for consent artifacts.
Store in a custom `consent_audit` table in Gold schema alongside OMOP tables.

---

## 4. Medication Coding Differences

| Scenario | FHIR system | Challenge | Resolution |
|----------|-------------|-----------|------------|
| Indian branded drugs | Often absent from RxNorm | Brand names like "Glycomet", "Januvia" not in standard RxNorm | Map via WHO INN → RxNorm; flag unmapped |
| Ayurvedic co-medication | No standard coding system | Patient-reported, not coded | Store as `drug_source_value` only |
| Generic substitution | RxNorm ingredient codes | Dispensed drug may differ from prescribed | Use `drug_type_concept_id = 38000177` (prescription) |

---

## 5. Diagnosis Coding Differences

| Scenario | Issue | Resolution |
|----------|-------|------------|
| ICD-10-CM (US) vs ICD-10 (India) | India uses ICD-10 (WHO), not ICD-10-CM (US) | Map to OMOP `ICD10` vocabulary, not `ICD10CM` |
| Local ICD modifications | Some Indian hospitals use ICD-10-2nd edition | Requires custom vocabulary mapping |
| SNOMED CT availability | Not universally coded in Indian EMRs | Accept ICD-10 as source; map to standard SNOMED concepts via OMOP |

---

## 6. Timezone Handling

ABDM requires timestamps in IST (UTC+5:30). FHIR stores as ISO 8601 with offset.

```python
# Convert ABDM timestamps to UTC for OMOP storage
from pyspark.sql import functions as F

F.to_utc_timestamp(
    F.to_timestamp("2024-01-01T10:30:00+05:30"),
    "Asia/Kolkata"
)
```

**OMOP convention:** Store all datetimes in UTC; document timezone in ETL metadata.

---

## TODO (Phase 3 Steps)

- [ ] Step 11: Complete this gap analysis after reading ABDM IG v6.5
- [ ] Step 12: Implement ABHA ID extraction in Silver patient model
- [ ] Step 13: Design consent_audit table schema
- [ ] Step 14: Build Indian drug name → RxNorm normalization lookup
- [ ] Step 15: Validate ICD-10 (WHO) vs ICD-10-CM mapping in OMOP concept table
