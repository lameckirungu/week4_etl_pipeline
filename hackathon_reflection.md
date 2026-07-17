# Hackathon #1 Reflection

**Analyst:** Lameck Irungu
**Cohort:** KPC Cohort, Inuka Fellowship
**Date:** June 2026

---

The biggest technical hurdle our team faced during Hackathon #1 was
data inconsistency at the point of merging. Each team member had cleaned
the same source dataset independently, but small differences in how we
handled zone labels — some left them as "Zone A", others converted to
"ZONE A", and one member removed the space entirely — meant that when we
tried to join our DataFrames, the merge produced three times as many rows
as expected. We lost nearly an hour diagnosing what looked like a logic
error but was actually a formatting problem we had each solved differently.

We resolved it by designating one person's cleaning function as the team
standard and re-running every member's analysis against that single
clean output. It was a straightforward fix once we identified the root
cause, but it reinforced something important: in a team environment, a
cleaning pipeline is not just a personal tool — it is a contract between
collaborators. Everyone downstream depends on it producing a predictable,
documented schema.

In the next hackathon I would push for a shared cleaning module to be
agreed and committed within the first fifteen minutes, before anyone
writes analysis code. I would also suggest a quick schema check — print
the column names and unique values in categorical columns — as a mandatory
first step for every team member after loading data. Most merge failures
trace back to assumptions about formatting that nobody thought to make
explicit. Making them explicit upfront costs five minutes and saves an hour.
