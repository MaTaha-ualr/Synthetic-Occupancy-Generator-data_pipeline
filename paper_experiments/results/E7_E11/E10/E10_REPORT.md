# E10: evidence-backed capability comparison

This is a structured literature comparison, not a performance experiment. The
cells describe capabilities explicitly evidenced in the cited primary papers,
official manuals, or official documentation reviewed on 2026-07-22.

**Legend:** ● = explicit first-class capability; ◐ = partial, indirect, or
extension-based capability; ○ = not evidenced in the reviewed sources. A ○ is
not proof that no version or private extension has the capability.

| Capability | SOG 2009 | Febrl | GeCo / DBGen | EMBench++ | pseudopeople | This work |
| --- | :---: | :---: | :---: | :---: | :---: | :---: |
| Temporal truth simulation | ● | ○ | ○ | ● | ● | ● |
| Household / lifecycle events | ◐¹ | ◐² | ◐² | ◐³ | ◐⁴ | ● |
| Multi-snapshot observation | ◐¹ | ○ | ○ | ● | ● | ● |
| Explicit 1:1 / 1:N / N:1 / M:N modes | ○ | ◐² | ◐² | ◐³ | ◐⁴ | ● |
| Dedup / pairwise / N-way topology controls | ○ | ◐² | ◐² | ◐³ | ◐⁴ | ● |
| Per-source noise profiles | ○¹ | ○² | ○² | ● | ● | ● |
| Name-collision control | ○ | ○ | ○ | ○ | ○ | ● |
| Canonical cluster truth map | ● | ● | ● | ● | ● | ● |
| Output-contract validation | ○ | ○ | ○ | ○ | ◐⁴ | ● |
| Deterministic hash-stable traits | ○ | ○ | ○ | ○ | ◐⁴ | ● |
| Open configurations | ◐¹ | ● | ● | ● | ● | ● |

The full operational definitions and machine-readable cells are in
[`e10_capability_matrix.csv`](e10_capability_matrix.csv); the bibliography and
the exact evidence used are in [`e10_sources.csv`](e10_sources.csv).

## Evidence notes

1. **SOG 2009.** The original SOG paper models temporal occupancy histories
   and single/couple histories, but describes systematic data disruption as
   future work. It is therefore not credited with the present implementation's
   source-specific observation layer. [Primary paper][sog09]
2. **Febrl and GeCo/DBGen.** These tools explicitly generate originals and one
   or more corrupted descendants and preserve the originating record in the
   identifier. That provides a useful 1:N subset and cluster truth, but not the
   four linkage cardinalities, N-way observation topology, or temporal
   household lifecycle as first-class controls. Family generation is described
   as hard-coded/extension logic, hence partial rather than absent.
   [Febrl manual][febrl] [GeCo paper][geco]
3. **EMBench++.** Its generic entity evolution, relationships,
   collections/sequences, per-dataset modifiers, configurations, and gold
   standard cover much of the abstract space. The reviewed source does not
   document the domain-specific household lifecycle, four record-linkage
   cardinality modes, or topology switches as first-class constructs.
   [EMBench++ paper][embench]
4. **pseudopeople.** It models families, households, employment, time, multiple
   observer datasets, and configurable per-dataset/per-column noise. Its
   simulant and household identifiers support truth recovery. The reviewed
   paper and documentation do not expose the present system's explicit
   cardinality/topology controls, collision target, or hash-stable trait
   contract; related schema/seed facilities receive partial credit.
   [Paper][pseudopeople-paper] [dataset docs][pseudopeople-data]
   [noise docs][pseudopeople-noise]

## What the comparison does and does not support

The table supports a **capability-composition** novelty claim: the reviewed
systems do not evidence the same combined surface of temporal household truth,
explicit cardinality and topology controls, source-specific noise,
name-collision control, canonical truth, executable output validation, and
hash-stable traits. It does not support a claim that every individual feature
is new. Several are established separately in earlier generators.

This framing protects the paper from overstating novelty. E7 also shows that a
wide capability surface does not by itself establish demographic realism, and
E11 separately tests whether one distinctive scenario creates measurably
different matching difficulty.

## Prior art to add to the related-work section

- Febrl and GeCo/DBGen for configurable corruption-based duplicate generation.
- EMBench++ for evolving entities, relationships, modifiers, and gold standards.
- pseudopeople and Synthea for longitudinal, openly generated populations.
- Magellan, DeepMatcher, and Ditto for rule-based, deep, and transformer entity
  matching systems against which the evaluation suite should be situated.
- Köpcke and Rahm; Köpcke, Thor, and Rahm; Menestrina et al.; and Christen's
  *Data Matching* for framework comparison and evaluation methodology.
- Bagga and Baldwin for cross-document entity/coreference evaluation history.
- North Carolina voter-registration data as an example of a public real-data
  benchmark. “NCVR” should be expanded as **North Carolina Voter Registration**,
  not National Change of Address.

The citation inventory in `e10_sources.csv` includes stable DOI or official
landing-page links for all of these additions.

[sog09]: https://www.researchgate.net/publication/215991472_SOG_A_Synthetic_Occupancy_Generator_to_Support_Entity_Resolution_Instruction_and_Research
[febrl]: https://users.cecs.anu.edu.au/~Peter.Christen/Febrl/febrl-0.3/febrldoc-0.3/node70.html
[geco]: https://doi.org/10.1145/2505515.2507815
[embench]: https://doi.org/10.3233/SW-180331
[pseudopeople-paper]: https://doi.org/10.12688/gatesopenres.15418.1
[pseudopeople-data]: https://pseudopeople.readthedocs.io/en/latest/datasets/
[pseudopeople-noise]: https://pseudopeople.readthedocs.io/en/latest/noise/index.html
