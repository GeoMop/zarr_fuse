# Mapping to Czech Core Metadata Model (WIP)

# TODO: relation to Attribute Convention for Data Discovery ([ACDD](https://wiki.esipfed.org/Attribute_Convention_for_Data_Discovery_1-3?utm_source=chatgpt.com))
Adapt zarr-fuse schema so that is could be mapped to ACDD, but possibly go beyond that.

# CCMM fields for publishing a zarr-fuse dataset (updated, practical extraction)

**Legend for “Auto-fill from zarr-fuse?” column**
- **Schema-derived** = can be computed from zarr-fuse schema + data arrays/axes (Time/Location/Quantity/Realization/Provenance axes, variable metadata).
- **Needs explicit attrs** = must be provided as explicit metadata attributes (at store/dataset/version/update level), or by repository during publication.
- **Repository-assigned** = typically minted/filled by repository at publish time (but you should prepare placeholders).

CCMM references:
- CCMM Research Data profile: https://model.ccmm.cz/research-data/en/
- CCMM vocabulary terms (types/properties): https://model.ccmm.cz/vocabulary/ccmm/
- TimeReference “Created” requirement is stated in the CCMM profile.  

---

## A) Minimal set — must provide (publishable + discoverable core)

| Field key | Type / structure | Auto-fill from zarr-fuse? | Definition (CCMM) | Example value (zarr-fuse dataset) |
|---|---|---|---|---|
| `Dataset.title` | `string` | Needs explicit attrs | Name given to the dataset. | `"DM4AI zarr-fuse: measured+sim hydrology dataset (v1)"` |
| `Dataset.hasIdentifier[]` | `Identifier[]` | Repository-assigned *(PID)* + Needs explicit attrs *(internal UUID)* | Unique identifier(s) of the dataset. | `[{scheme:"https://doi.org", notation:"10.1234/dm4ai.zarrfuse.001.v1"}, {scheme:"urn:uuid", notation:"8c02f3e8-5d1a-4c6f-9f4a-4a0d0d9e0f61"}]` |
| `Dataset.hasDescription[]` | `Description[]` | Needs explicit attrs *(but can be templated)* | Additional information (free text). | `[{descriptionText:"Zarr dataset assembled by zarr-fuse; axes: time, location, realization, provenance, quantity; includes QC steps and simulation run-set A."}]` |
| `Dataset.hasSubject[]` | `Subject[]` | Needs explicit attrs *(but can be suggested from Quantity list)* | Subject/keywords describing dataset. | `[{title:"hydrology"}, {title:"time series"}, {title:"surrogate models"}]` |
| `Dataset.hasTermsOfUse.accessRights.iri` | `uri` | Needs explicit attrs | Conditions to access resource (access rights). | `"http://purl.org/coar/access_right/c_abf2"` *(open)* |
| `Dataset.hasTermsOfUse.license.iri` | `uri` | Needs explicit attrs | License granting permission to use the resource. | `"https://creativecommons.org/licenses/by/4.0/"` |
| `Dataset.hasDistribution[]` | `Distribution[]` | Repository-assigned *(URLs)* + Needs explicit attrs *(format)* | Available distribution(s) for dataset. | `[{accessURL:["https://repo.example.cz/records/dm4ai-001"], format:"zarr"}]` |
| `Dataset.hasDistribution[].accessURL` | `uri[]` | Repository-assigned | URL giving access to a distribution (landing page / endpoint). | `["https://repo.example.cz/records/dm4ai-001"]` |
| `Dataset.hasDistribution[].format` | `string` | Schema-derived | File format of the distribution. | `"zarr"` |

---

## B) Definitely relevant — strongly recommended (NRP/NMA-friendly + good practice)

| Field key | Type / structure | Auto-fill from zarr-fuse? | Definition (CCMM) | Example value (zarr-fuse dataset) |
|---|---|---|---|---|
| `Dataset.version` | `string` | Schema-derived *(if versioning is part of store)* OR Needs explicit attrs | Version indicator. | `"1.0.0"` |
| `Dataset.publicationYear` | `gYear` | Repository-assigned OR Needs explicit attrs | Year dataset is made publicly available. | `2026` |
| `Dataset.hasTimeReference[]` | `TimeReference[]` | **Schema-derived** *(Created interval from Time axis)* + Repository-assigned *(Issued)* | Time reference for events related to resource. CCMM requires at least one with date type “Created”. | `[{"dateType":".../TimeReference/Created","temporal":{"type":"interval","begin":"2025-01-01","end":"2025-12-31"}},{"dateType":".../TimeReference/Issued","temporal":{"type":"instant","at":"2026-03-01"}}]` |
| `Dataset.hasLocation[]` | `Location[]` | **Schema-derived** *(from Location axis)* | Geographical area covered by dataset. | `[{type:"bbox", value:[14.25,49.90,14.75,50.20]}, {type:"site", name:"Station-Karlov", lat:50.08, lon:14.43}]` |
| `Dataset.qualifiedRelation[]` | `ResourceToAgentRelationship[]` | Needs explicit attrs | Qualified relation between resource and agent with role (creator/publisher/contact…). | `[{role:"creator", agent:{...}}, {role:"publisher", agent:{...}}, {role:"contactPoint", agent:{...}}]` |
| `Dataset.hasProvenance[]` | `ProvenanceStatement[]` | Needs explicit attrs *(but can be partially derived from operations log)* | Statement about lineage of dataset. | `[{statement:"v1.0 merged station logs (raw→QC) + simulations (run-set A); schema dm4ai_dataset.yaml#v1; chunking 30d; provenance axis carries sensor_id/sim_id."}]` |
| `Dataset.hasRelatedResource[]` | `RelatedResource[]` | Needs explicit attrs *(URLs)* | Reference to any related resource (software, protocol, paper, model description, upstream dataset). | `[{url:"https://github.com/GeoMop/zarr_fuse", relation:"software"}, {url:"https://example.cz/protocols/calibration-v2.pdf", relation:"method"}]` |
| `Dataset.hasDistribution[].conformsToSchema[]` | `uri[]` | Schema-derived *(schema URL/version)* | Standard/model/schema the distribution conforms to. | `["https://github.com/GeoMop/zarr_fuse/blob/main/schema/dm4ai_dataset.yaml#v1"]` |

---

## C) Possibly relevant — extensive list (use when available / safely derivable)

### C1) Dataset description enrichment / discovery

| Field key | Type / structure | Auto-fill from zarr-fuse? | Definition (CCMM) | Example value |
|---|---|---|---|---|
| `Dataset.hasDescription[].hasDescriptionType.iri` | `uri` | Needs explicit attrs | Type of description (controlled concept). | `"https://vocabs.ccmm.cz/.../DescriptionType/TechnicalInfo"` |
| `Dataset.hasSubject[].classificationCode` | `string` | Needs explicit attrs | Classification code for subject term (if using a scheme). | `"GCMD:EARTH SCIENCE > HYDROSPHERE > HYDROLOGY"` |
| `Dataset.hasSubject[].inSubjectScheme.iri` | `uri` | Needs explicit attrs | Subject scheme (thesaurus/vocabulary) that subject belongs to. | `"https://gcmd.earthdata.nasa.gov/kms/concepts/concept_scheme/sciencekeywords"` |
| `Dataset.hasPrimaryLanguage.iri` | `uri` | Needs explicit attrs | Primary language of metadata. | `"http://publications.europa.eu/resource/authority/language/ENG"` |
| `Dataset.hasOtherLanguage[].iri` | `uri[]` | Needs explicit attrs | Other languages of metadata. | `["http://publications.europa.eu/resource/authority/language/CES"]` |

### C2) Distribution integrity, size, direct downloads, services/API

| Field key | Type / structure | Auto-fill from zarr-fuse? | Definition (CCMM) | Example value |
|---|---|---|---|---|
| `Dataset.hasDistribution[].downloadURL` | `uri[]` | Repository-assigned | Direct link to downloadable file/object. | `["https://repo.example.cz/files/dm4ai-001.zarr.zip"]` |
| `Dataset.hasDistribution[].byteSize` | `integer` | Schema-derived *(compute)* | Size in bytes. | `184467440737095516` |
| `Dataset.hasDistribution[].hasChecksum` | `Checksum` | Schema-derived *(compute)* | Checksum for integrity verification. | `{algorithm:"sha256", value:"b1946ac9..."}`
| `Dataset.hasDistribution[].title` | `string` | Needs explicit attrs | Name given to distribution. | `"Zarr store (HTTP) — v1.0.0"` |
| `Dataset.hasDistribution[].hasAccessService[]` | `DataService[]` | Repository-assigned OR Needs explicit attrs | Service/API endpoint giving access. | `[{endpoint:"https://repo.example.cz/api/zarr/dm4ai-001", protocol:"https"}]` |
| `Dataset.hasDistribution[].hasDocumentation[]` | `uri[]` | Needs explicit attrs | Documentation about distribution/service. | `["https://github.com/GeoMop/zarr_fuse/tree/main/docs"]` |

### C3) Agents / authors / responsibilities (rich detail)

| Field key | Type / structure | Auto-fill from zarr-fuse? | Definition (CCMM) | Example value |
|---|---|---|---|---|
| `Dataset.qualifiedRelation[].hadRole.iri` | `uri` | Needs explicit attrs | Role of agent w.r.t. resource (creator, publisher, rightsHolder, contributor, contactPoint…). | `"https://vocabs.ccmm.cz/.../ResourceAgentRole/creator"` |
| `Dataset.qualifiedRelation[].hasRelatedAgent` | `Agent` | Needs explicit attrs | Agent (Person/Organization). | `{type:"Person", givenName:["Jan"], familyName:["Novák"], identifiers:[...]}`
| `Dataset.hasContactPoint[]` *(if used separately)* | `Agent[]` | Needs explicit attrs | Contact point for dataset (support/helpdesk). | `[{type:"Organization", name:"DM4AI Support", contact:{email:["dm4ai@org.cz"]}}]` |

### C4) Rights / access nuance

| Field key | Type / structure | Auto-fill from zarr-fuse? | Definition (CCMM) | Example value |
|---|---|---|---|---|
| `Dataset.hasTermsOfUse.description` | `Text[]` | Needs explicit attrs | Human-readable details of rights/licensing. | `[{lang:"en", value:"Open data; cite dataset PID and version; embargo not used."}]` |
| `Dataset.hasTermsOfUse.hasContactPoint[]` | `Agent[]` | Needs explicit attrs | Contact for terms-of-use questions. | `[{type:"Organization", name:"Legal Office", contact:{email:["legal@org.cz"]}}]` |

### C5) Funding / project context

| Field key | Type / structure | Auto-fill from zarr-fuse? | Definition (CCMM) | Example value |
|---|---|---|---|---|
| `Dataset.hasFundingReference[]` | `FundingReference[]` | Needs explicit attrs | Funding reference for dataset acquisition. | `[{awardTitle:"Open Science II – DM4AI", localIdentifier:"DM4AI-2025", funder:{name:"MŠMT"}}]` |
| `Dataset.inProject[]` *(if present in profile)* | `Project[]` | Needs explicit attrs | Project association. | `[{name:"EOSC CZ / DM4AI"}]` |

### C6) Validation / QA evidence (maps well to zarr-fuse validation)

| Field key | Type / structure | Auto-fill from zarr-fuse? | Definition (CCMM) | Example value |
|---|---|---|---|---|
| `Dataset.hasValidationResult[]` | `ValidationResult[]` | Schema-derived *(if you generate reports)* OR Needs explicit attrs | Result of validation testing (metadata/data/services). | `[{title:"Schema+metadata validation v1", url:"https://repo.example.cz/files/validation-report.json"}]` |

### C7) Provenance (structured operations log — recommended for zarr-fuse)

| Field key | Type / structure | Auto-fill from zarr-fuse? | Definition (CCMM) | Example value |
|---|---|---|---|---|
| `Dataset.hasProvenance[].statement` | `string` | Needs explicit attrs *(can be auto-generated summary)* | Lineage statement. | `"Derived from station CSV logs (raw) → QC filter → unit conversion → merged into zarr-fuse store; simulations imported from run-set A (commit abc123)."` |
| `Dataset.hasProvenance[].hasRelatedResource[]` | `RelatedResource[]` | Needs explicit attrs | Link provenance to workflow docs, code, config bundles, upstream datasets. | `[{url:"https://example.cz/workflows/qc-v3", relation:"workflow"}, {url:"https://github.com/org/simcode/releases/tag/v2.1", relation:"software"}]` |
| `Dataset.hasProvenance[].operations[]` *(project convention)* | `Operation[]` *(not CCMM-mandated; recommended convention)* | Schema-derived *(if you log ops)* + Needs explicit attrs *(operator identity)* | A list of operations (activity log) used to form the dataset/version. | `[{op:"ingest", at:"2026-02-10T12:01Z", inputs:["raw://station/2025/*.csv"], tool:"zarr-fuse 0.3.1"}, {op:"qc", params:{ruleSet:"QC-v3"}, tool:"qc-pipeline 1.2"}]` |

> Note: CCMM defines provenance as a lineage statement; the structured `operations[]` is a practical extension you can maintain internally and also serialize into `statement` + `hasRelatedResource` for CCMM export.

---

# Complex types breakdown (internal keys)

## `Identifier` (used by `Dataset.hasIdentifier[]`, `Agent.hasIdentifier[]`)
| Key | Type | Auto-fill from zarr-fuse? | Meaning / notes | Example |
|---|---|---|---|---|
| `Identifier.notation` | `string` | Needs explicit attrs OR Repository-assigned | Identifier value. | `"10.1234/dm4ai.zarrfuse.001.v1"` |
| `Identifier.inScheme.iri` | `uri` | Needs explicit attrs | Scheme in which the value is unique (authority). | `"https://doi.org"` |
| `Identifier.authorized` | `boolean` | Repository-assigned | Whether identifier is authorized by the subject it represents. | `true` |

## `Description` (used by `Dataset.hasDescription[]`)
| Key | Type | Auto-fill from zarr-fuse? | Meaning / notes | Example |
|---|---|---|---|---|
| `Description.descriptionText` | `string` | Needs explicit attrs *(templatable)* | Description text. | `"Axes: time, location, realization, provenance, quantity; QC applied; sim run-set A included."` |
| `Description.hasDescriptionType.iri` | `uri` | Needs explicit attrs | Type of description (if used). | `"…/DescriptionType/TechnicalInfo"` |

## `Subject` (used by `Dataset.hasSubject[]`)
| Key | Type | Auto-fill from zarr-fuse? | Meaning / notes | Example |
|---|---|---|---|---|
| `Subject.title` | `string` | Needs explicit attrs *(suggested from quantities)* | Keyword/subject label. | `"hydrology"` |
| `Subject.classificationCode` | `string` | Needs explicit attrs | Code within subject scheme (optional). | `"GCMD:..."` |
| `Subject.inSubjectScheme.iri` | `uri` | Needs explicit attrs | Scheme/vocabulary identifier. | `"https://gcmd.earthdata.nasa.gov/.../sciencekeywords"` |

## `TermsOfUse` (used by `Dataset.hasTermsOfUse`)
| Key | Type | Auto-fill from zarr-fuse? | Meaning / notes | Example |
|---|---|---|---|---|
| `TermsOfUse.accessRights.iri` | `uri` | Needs explicit attrs | Access rights concept. | `"http://purl.org/coar/access_right/c_abf2"` |
| `TermsOfUse.license.iri` | `uri` | Needs explicit attrs | License URL/IRI. | `"https://creativecommons.org/licenses/by/4.0/"` |
| `TermsOfUse.description` | `Text[]` | Needs explicit attrs | Human-readable rights notes. | `[{lang:"en", value:"Open; cite PID+version."}]` |
| `TermsOfUse.hasContactPoint[]` | `Agent[]` | Needs explicit attrs | Contact for rights questions. | `[{type:"Organization", name:"Legal Office", contact:{email:["legal@org.cz"]}}]` |

## `Distribution` (used by `Dataset.hasDistribution[]`)
| Key | Type | Auto-fill from zarr-fuse? | Meaning / notes | Example |
|---|---|---|---|---|
| `Distribution.accessURL` | `uri[]` | Repository-assigned | Landing page / endpoint. | `["https://repo.example.cz/records/dm4ai-001"]` |
| `Distribution.downloadURL` | `uri[]` | Repository-assigned | Direct download link(s). | `["https://repo.example.cz/files/dm4ai-001.zarr.zip"]` |
| `Distribution.format` | `string` | Schema-derived | File format. | `"zarr"` |
| `Distribution.byteSize` | `integer` | Schema-derived | Size in bytes. | `184467...` |
| `Distribution.hasChecksum` | `Checksum` | Schema-derived | Integrity checksum. | `{algorithm:"sha256", value:"..."}` |
| `Distribution.title` | `string` | Needs explicit attrs | Distribution name. | `"Zarr store — v1"` |
| `Distribution.conformsToSchema[]` | `uri[]` | Schema-derived | Schema/profile reference. | `["https://.../dm4ai_dataset.yaml#v1"]` |
| `Distribution.hasAccessService[]` | `DataService[]` | Repository-assigned OR Needs explicit attrs | API/service endpoints. | `[{endpoint:"https://.../api/zarr/dm4ai-001"}]` |

## `TimeReference` (used by `Dataset.hasTimeReference[]`)
| Key | Type | Auto-fill from zarr-fuse? | Meaning / notes | Example |
|---|---|---|---|---|
| `TimeReference.hasDateType.iri` | `uri` | Needs explicit attrs | Date type concept (Created/Issued/etc.). | `"…/TimeReference/Created"` |
| `TimeReference.hasTemporalRepresentation` | `object` | Schema-derived *(Created)* | Time representation (instant/interval). | `{type:"interval", begin:"2025-01-01", end:"2025-12-31"}` |
| `TimeReference.dateInformation` | `string` | Needs explicit attrs | Extra human-readable info. | `"Created interval derived from Time axis."` |

## `ResourceToAgentRelationship` (used by `Dataset.qualifiedRelation[]`)
| Key | Type | Auto-fill from zarr-fuse? | Meaning / notes | Example |
|---|---|---|---|---|
| `ResourceToAgentRelationship.hadRole.iri` | `uri` | Needs explicit attrs | Role concept (creator/publisher/contact…). | `"…/ResourceAgentRole/creator"` |
| `ResourceToAgentRelationship.hasRelatedAgent` | `Agent` | Needs explicit attrs | Agent in that role. | `{type:"Person", ...}` |

## `Agent` + `Person` + `Organization` (used under qualified relations, contacts, funders)
| Key | Type | Auto-fill from zarr-fuse? | Meaning / notes | Example |
|---|---|---|---|---|
| `Agent.type` | `enum` | Needs explicit attrs | Person or Organization. | `"Person"` |
| `Agent.hasIdentifier[]` | `Identifier[]` | Needs explicit attrs | ORCID/ROR/ISNI/etc. | `[{scheme:"https://orcid.org", notation:"0000-0002-1234-5678"}]` |
| `Agent.contactPoint[]` | `ContactDetails[]` | Needs explicit attrs | Contact info bundle. | `[{email:["dm4ai@org.cz"]}]` |
| `Person.givenName[]` | `string[]` | Needs explicit attrs | Given name(s). | `["Jan"]` |
| `Person.familyName[]` | `string[]` | Needs explicit attrs | Family name(s). | `["Novák"]` |
| `Person.hasAffiliation` | `Organization` | Needs explicit attrs | Affiliated organization. | `{name:"University X"}` |
| `Organization.name` | `string` | Needs explicit attrs | Organization name. | `"DM4AI / CESNET"` |
| `Organization.alternateName[]` | `Text[]` | Needs explicit attrs | Alternate names. | `[{lang:"cs", value:"Ústav ..."}]` |

## `ContactDetails`
| Key | Type | Auto-fill from zarr-fuse? | Meaning / notes | Example |
|---|---|---|---|---|
| `ContactDetails.email[]` | `string[]` | Needs explicit attrs | Email address(es). | `["dm4ai@org.cz"]` |
| `ContactDetails.phone[]` | `string[]` | Needs explicit attrs | Phone number(s). | `["+420111222333"]` |
| `ContactDetails.dataBox` | `string` | Needs explicit attrs | Data box code (CZ context). | `"abcd123"` |

## `RelatedResource` (used by `Dataset.hasRelatedResource[]` and provenance links)
| Key | Type | Auto-fill from zarr-fuse? | Meaning / notes | Example |
|---|---|---|---|---|
| `RelatedResource.url` | `uri` | Needs explicit attrs | Resolvable URL representing related resource. | `"https://github.com/GeoMop/zarr_fuse"` |
| `RelatedResource.relation` | `string` | Needs explicit attrs | Informal relation type (software/method/protocol/upstream…). | `"software"` |

## `Location` (used by `Dataset.hasLocation[]`)
| Key | Type | Auto-fill from zarr-fuse? | Meaning / notes | Example |
|---|---|---|---|---|
| `Location.type` | `enum` | Schema-derived | Representation type (bbox/site/geometry/admin). | `"bbox"` |
| `Location.value` | `number[]` | Schema-derived | Coordinates (e.g., bbox). | `[14.25,49.90,14.75,50.20]` |
| `Location.name` | `string` | Schema-derived OR Needs explicit attrs | Site label (if stored). | `"Station-Karlov"` |
| `Location.lat` / `Location.lon` | `number` | Schema-derived | Site coordinates. | `50.08` / `14.43` |

## `ProvenanceStatement`
| Key | Type | Auto-fill from zarr-fuse? | Meaning / notes | Example |
|---|---|---|---|---|
| `ProvenanceStatement.statement` | `string` | Needs explicit attrs *(can be generated summary)* | Lineage statement. | `"raw station CSV → QC-v3 → unit conversion → merge; plus sims run-set A (commit abc123)."` |
| `ProvenanceStatement.hasRelatedResource[]` | `RelatedResource[]` | Needs explicit attrs | Links to workflow/code/config inputs. | `[{url:"https://.../qc-v3", relation:"workflow"}]` |
| `ProvenanceStatement.operations[]` *(convention)* | `Operation[]` | Schema-derived *(if you log)* | Structured internal log (optional). | `[{op:"qc", at:"...", tool:"qc-pipeline 1.2"}]` |

## `FundingReference`
| Key | Type | Auto-fill from zarr-fuse? | Meaning / notes | Example |
|---|---|---|---|---|
| `FundingReference.awardTitle` | `string` | Needs explicit attrs | Award/grant title. | `"Open Science II – DM4AI"` |
| `FundingReference.localIdentifier` | `string` | Needs explicit attrs | Award identifier. | `"DM4AI-2025"` |
| `FundingReference.hasFunder` | `Organization` | Needs explicit attrs | Funder organization. | `{name:"MŠMT"}` |

## `ValidationResult`
| Key | Type | Auto-fill from zarr-fuse? | Meaning / notes | Example |
|---|---|---|---|---|
| `ValidationResult.title` | `string` | Schema-derived OR Needs explicit attrs | Name of validation report. | `"Schema+metadata validation v1"` |
| `ValidationResult.url` | `uri` | Schema-derived OR Needs explicit attrs | Location of report artifact. | `"https://repo.example.cz/files/validation-report.json"` |

## `Checksum`
| Key | Type | Auto-fill from zarr-fuse? | Meaning / notes | Example |
|---|---|---|---|---|
| `Checksum.algorithm` | `string` | Schema-derived | Hash algorithm. | `"sha256"` |
| `Checksum.value` | `string` | Schema-derived | Hash value. | `"b1946ac92492..."` |

## `DataService` (if modeling API access)
| Key | Type | Auto-fill from zarr-fuse? | Meaning / notes | Example |
|---|---|---|---|---|
| `DataService.endpoint` | `uri` | Repository-assigned OR Needs explicit attrs | Service/API endpoint URL. | `"https://repo.example.cz/api/zarr/dm4ai-001"` |
| `DataService.protocol` | `string` | Needs explicit attrs | Protocol identifier. | `"https"` |

---
