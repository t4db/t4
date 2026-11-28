------------------------------- MODULE Election -------------------------------
EXTENDS Integers, Sequences, TLC

CONSTANTS Nodes, MaxTerm

VARIABLES lock, call, won
Vars == <<lock, call, won>>

\* lock = <<owner, election term, revision, WAL sequence, WAL term>>
\* call = <<operation, node, floor term, revision, WAL sequence, WAL term, expired>>
\* Operation 1 is startup acquisition; 2 is takeover. Owner 0 means absent.
Empty == <<0, 0, 0, 0, 0>>
NoCall == <<0, 0, 0, 0, 0, 0, FALSE>>

\* Legacy revisions and modern WAL positions intentionally give different orderings.
Fences == {<<0, 0, 0>>, <<1, 0, 0>>, <<2, 0, 0>>,
           <<1, 1, 1>>, <<2, 2, 1>>, <<0, 1, 2>>}

Init == /\ lock = Empty
        /\ call = NoCall
        /\ won = FALSE

CoversFence(candidate) ==
    IF lock[4] = 0 /\ lock[5] = 0
    THEN candidate[1] >= lock[3]
    ELSE \/ candidate[3] > lock[5]
         \/ /\ candidate[3] = lock[5]
            /\ candidate[2] >= lock[4]

Attempt(operation, node, floor, candidate, expired) ==
    LET ownOrAbsent == lock[1] \in {0, node}
        mayWrite == \/ ownOrAbsent
                    \/ /\ operation = 2
                       /\ expired
                       /\ floor >= lock[2]
                       /\ CoversFence(candidate)
        nextTerm == IF floor >= lock[2] THEN floor + 1 ELSE lock[2] + 1
    IN /\ (~mayWrite \/ nextTerm <= MaxTerm)
       /\ call' = <<operation, node, floor, candidate[1], candidate[2],
                    candidate[3], expired>>
       /\ won' = mayWrite
       /\ lock' = IF won'
                  THEN <<node, nextTerm, candidate[1], candidate[2], candidate[3]>>
                  ELSE lock

Next == \E node \in Nodes, floor \in 0..(MaxTerm - 1), candidate \in Fences:
            \/ Attempt(1, node, floor, candidate, FALSE)
            \/ \E expired \in BOOLEAN: Attempt(2, node, floor, candidate, expired)

TypeOK ==
    /\ lock \in ({0} \cup Nodes) \X (0..MaxTerm) \X (0..2) \X (0..2) \X (0..2)
    /\ call \in (0..2) \X ({0} \cup Nodes) \X (0..(MaxTerm - 1))
                \X (0..2) \X (0..2) \X (0..2) \X BOOLEAN
    /\ won \in BOOLEAN

TermNeverRegresses == [][lock'[2] >= lock[2]]_Vars
RefusalPreservesLock == [][~won' => lock' = lock]_Vars
TakeoverRespectsFence ==
    [][(call'[1] = 2 /\ lock[1] \notin {0, call'[2]} /\ won') =>
       (call'[7] /\ call'[3] >= lock[2] /\ CoversFence(<<call'[4], call'[5], call'[6]>>))]_Vars

=============================================================================
