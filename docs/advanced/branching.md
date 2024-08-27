# What is Branching?

When a `StreamingDataFrame` (`SDF`) diverges from a single point into two or more independent 
processing points, we call this `branching`.

## Simple Graph Representation

Assume we have a `StreamingDataFrame`, `sdf` where each letter `X` is
some operation added to it like `sdf = sdf.apply()`.

Before branching, only this was possible:

```
sdf
└── A
    └── B
        └── C
            └── D
```

But with branching, you could do something like:

```
sdf
└── A
    └── B
        ├── C
        │   └── D
        └── E
            ├── F
            ├── G
            └── H
```

# Branching Fundamentals

## Generating Branches
Branches can be generated by adding `>1` operations to the same variable instance
of an `SDF` _in separate assignment steps_.

That is, rather than reassigning new operation(s) to _the same_ node/variable, 
you assign them to a _new one_!

```python
sdf_0 = app.dataframe().apply(func_a)
sdf_0 = sdf_0.apply(func_b)  # sdf_0 -> sdf_0: NOT a (new) branch
sdf_1 = sdf_0.apply(func_c)  # sdf_0 -> sdf_1: generates new branch off sdf_0
sdf_2 = sdf_0.apply(func_d)  # sdf_0 -> sdf_2: generates new branch off sdf_0
```

There is no limit to the number of branches you can generate at a given node/variable.

> **side note**: if branches assigned to a given node are not `>1` (ex: if `sdf_2` was 
> not added), then the new "branch" will still be treated as if it's not one
> (and still be applied as normal).

Branches can also be added at any point, they do not need to follow immediately after
another:

```python
sdf_0 = app.dataframe().apply(func_a)
sdf_0 = sdf_0.apply(func_b)  # sdf_0 -> sdf_0: NOT a new branch (continues sdf_0)
sdf_1 = sdf_0.apply(func_c)  # sdf_0 -> sdf_1: generates new branch off current sdf_0
sdf_2 = sdf_0.apply(func_d)  # sdf_0 -> sdf_2: generates new branch off current sdf_0
sdf_0 = sdf_0.apply(func_e)  # sdf_0 -> sdf_0: NOT a new branch (continues sdf_0)
sdf_3 = sdf_0.apply(func_f)  # sdf_0 -> sdf_3: generates new branch off current sdf_0
```

## Branching vs. Chaining

Branching is independent of _chaining_, where chaining simply combines multiple 
operations together on a single line like `sdf = sdf.apply().filter().print()`.

Chaining can be used alongside branching.

## Branching vs. Multiple Topics (Multi-SDF)

[Consuming multiple topics](consuming-multiple-topics.md) is independent of branching;
branches can be used in each of the `SDF`s from multiple topics, but they cannot
interact with one another in any way.

### Clarifying Multiple SDFs
`SDF`'s are delineated by the topic they are initialized with;
branches just generate additional nodes you can manipulate on them, 
but they are all still part of the same `SDF` (from the user perspective). 


## Branch Leafs and Tracking

Also being introduced alongside branching is automated `SDF` tracking.

Basically, any `SDF` generated with `Application.dataframe()` will be 
tracked for you, along with any of its branches.

This means `Application.run()` automatically detects all `SDF` (and their branches)
and executes them.

## Branching Limitations

Most of branching's limitations are around:
- [using a branch to manipulate another](#branch-interactions)
- [shared SDF state](#using-with-state)

# Basic Branching Example

What does that look like in practice?

Here is a simple example showcasing branching:

```python
from quixstreams import Application

def add(n):
    """
    generates adding functions rather than use lambdas
    """
    def add_n(value):
        return value + n
    return add_n

app = Application("localhost:9092")
input_topic = app.topic("in", value_deserializer="int")
output_topic = app.topic("out", value_serializer="int")

sdf_0 = app.dataframe(input_topic)
sdf_0 = sdf_0.apply(add(10)).apply(add(20))
sdf_1 = sdf_0.apply(add(70)).apply(add(1)).to_topic(output_topic)
sdf_2 = sdf_0.apply(add(102)).to_topic(output_topic)
sdf_0 = sdf_0.apply(add(500)).to_topic(output_topic)

app.run()
```

From this, processing the value `0` would produce 3 messages to `output_topic` in the
following order:

```python
101  # sdf_1: (10 + 20 + 70 + 1)
132  # sdf_2: (10 + 20 + 102)
530  # sdf_0: (10 + 20 + 500)
```

or, graphically:

```
SDF
└── 0 (input)
    └── (+ 10 + 20)
        ├── (+ 70 + 1) = 101
        ├── (+ 102   ) = 132
        └── (+ 500   ) = 530
```


# Execution Ordering

Because of the way branches are tracked and interact, ordering can be tricky.

We make a best effort to execute the SDF **in the order the operations were added**,
or essentially from "top to bottom".

That said, if you have behavior that relies on a specific execution ordering, 
it is recommended to test it and ensure its occurring in the order you expect, as
there may be uncaught edge cases. 

## Ordering example

Referring to the [branching example](#simple-example), the branch
results are produced in the following order:

1. `sdf_1`
2. `sdf_2`
3. `sdf_0`

Interpreted another way, the "last" operation added to each branch occurred in this order.

## Ordering with `expand=True`

Using `expand=True` (like with `SDF.apply(f, expand=True)`) also produces its results
in a specific order: each element of the expand is fully processed before handling the 
next.

#### Expand Example Snippet:

Imagine you had the value `[0, 1]` and `sdf` that branched like so:

```python
sdf = sdf.apply(lambda x: x, expand=True)  # process list per-element
sdf_a = sdf.apply(add(10)).to_topic(output_topic)
sdf_b = sdf.apply(add(20)).to_topic(output_topic)
```

The 4 produced messages would occur in the following order:

```python
10  # 0 via sdf_a: 0 + 10
20  # 0 via sdf_b: 0 + 20
11  # 1 via sdf_a: 1 + 10
21  # 1 via sdf_b: 1 + 20
```
or graphically, 

```
SDF
└── [0, 1] INPUT
    └── EXPAND
        ├── 0
        │   ├── + 10 = 10
        │   └── + 20 = 20
        └── 1
            ├── + 10 = 11
            └── + 20 = 21
```

# Using with State

While most state functionality works with `branching`, there is one situation to be 
aware of.


## Custom State (`stateful=True`)

Performing `SDF` operations that use `stateful = True` is something to
be careful with, as **stateful operations share the same state between one another**.

For example, assume:
- two branches from the same node: `branch_A` and `branch_B` 
  - each use `.apply(f, stateful = True)`
    - the `f` uses `.set()`/`.get()` on key `my_key`
  - `branch_B` executes after `branch_A`

In this situation, `branch_B`'s `my_key` lookup would be the result of whatever 
`branch_A` did since the state is shared through all SDF branches.

### Custom state and multi-topic Applications

This "shared" state concern is NOT applicable across the multiple `SDF`'s used for 
multiple topic consuming, as _`SDF` state is tied to its topic name_ 
(among other things).


# Branch Interactions

**Branches should be treated as independent entities that cannot be "combined" 
or "interact".**

The most common ways this is likely to occur is filtering and column assigning:

```python
# Column Assigning
sdf_b["new_col"] = sdf_a.apply(f)

# Column Filtering
sdf_b = sdf_b[sdf_a.apply(g)]
```

Since there are still valid manipulations with these approaches (corresponding 
to non-branching), it may be tricky to identify valid versus invalid usage.

**If you don't wish to go into too much detail, just take these into consideration**:
1. Use the same `SDF` instance as both the source _and_ operation.
    - Valid: 
      - `sdf_1 = sdf_0[sdf_0.apply(f)]` (can even branch if you wish!)
      - `sdf_0['new_col'] = sdf_0.apply(f)`
    - Invalid: 
      - `sdf_2 = sdf_1[sdf_0.apply(f)]` (`sdf_1` != `sdf_0`)
      - `sdf_1['new_col'] = sdf_0.apply(f)` (`sdf_1` != `sdf_0`)
2. Avoid assigning filters or assignors for later use:
    - Example: 
      ```
      my_filter = sdf.apply(my_func)
      sdf = sdf[my_filter]
      ```
3. Most common invalid usage _should_ raise exceptions
    - validate results manually if in question

Optionally, check out the [advanced breakdown](#advanced-concepts) for more details 
regarding invalid interactions.

# Performance

Branching does have some performance implications. 

## Data Cloning

Any nodes with branches require cloning the current value at that node `N-1` 
times, where `N` is number of branches at a given node (though any subsequent clones 
are much cheaper relative to the first).

### Cloning Limitations

Data cloning is done using `pickle`, which does mean that it's possible for
the value cloning to fail if the data cannot be serialized with `pickle`.

Though unlikely, if an exception is encountered, try changing the message value to 
a different format before the branching occurs.

# Advanced Concepts

An entirely optional section to better understand the internals of branching.

## Invalid Interactions

> NOTE: this section gets a bit more technical! 
> See [branch interactions](#branch-interactions) for a basic ruleset that
> covers most cases.

For this section, assume the following example:

```python
sdf_0 = sdf_0.apply(func_q)
sdf_1 = sdf_0.apply(func_r)
sdf_2 = sdf_0.apply(func_s)
sdf_3 = sdf_1.apply(func_t)
sdf_4 = sdf_1.apply(func_u)
sdf_5 = sdf_2.apply(func_v)
```

graph, showing each:
```
sdf_0
├── sdf_1
│   ├── sdf_3
│   └── sdf_4
└── sdf_2
    └── sdf_5
```

### Nodes

Each independent `SDF` (`sdf_N`) can be considered a **node**.

### Parent, Children, and Ancestors

A node's **parent** is the node it spawned from, and conversely a node's **children** 
are all the nodes spawned directly from it.

**Ancestors** are basically all nodes that occur while recursively traversing parents
until you hit the "end" (node without a parent).

Example:

- `sdf_1`
  - parent: `sdf_0`
  - children: \[`sdf_3`, `sdf_4`\]
  - ancestors: `sdf_0`

- `sdf_0`
  - parent: `None`
  - children: \[`sdf_1`, `sdf_2`\]
  - ancestors: `None`

- `sdf_3`
  - parent: `sdf_1`
  - children: `None`
  - ancestors: \[`sdf_1`, `sdf_0`\]


### Branches

A node is a **branch** if its parent has >1 child.

### Node Origin

A node's origin is essentially _its first ancestor that has >1 children_.

Importantly, it may _not_ be a node's parent until it becomes a branch.

A good example of this is `sdf_5` (snippet of example):

```
sdf_0
├── sdf_1
└── sdf_2
    └── sdf_5
```
- parent: `sdf_2`
- origin: `sdf_0`

but if we added:

```python
sdf_6 = sdf_2.apply(f)
```
```
sdf_0
├── sdf_1
└── sdf_2
    ├── sdf_5
    └── sdf_6
```

then `sdf_5` origin changes to `sdf_2` which now has `>1` children 
(also making `sdf_5` a branch).

### Pruning

Pruning is when we remove a node (in its entirety) from the "tree" and compose its
operations (starting from the pruned node and later). 

This is followed by applying this composition back as a single new operation to the 
node doing the pruning.

Pruning occurs during filtering and column assignment operations, and **is allowed only
when two nodes have the same origin**. Why?

Pruned operations are entirely autonomous, as if steps from a recipe were cut 
and pasted onto another with no edits: they are a context-less collection of functions.
As such, they must share the same "context" (origin) for their use to make sense.

#### Pruning example

Imagine we had a filtering operation: `sdf_A[sdf_B.apply(f)]`

If `sdf_B` does not share the same origin as `sdf_A`, the pruned operations from 
`sdf_B` (`.apply(f)`) will result in adding said operations to `sdf_A` that only make 
sense with the context of `sdf_B`.


### Invalid Filtering and Column Assigning

Pruning behavior is basically what invalidates interaction between various
branches. There are some edge cases that are technically valid, but they can
basically be accomplished otherwise and are generally not recommended.

### Filters and Assignors as Variables

Pruning is also generally why storing intermediate filters and assignors as variables 
is not recommended: it's more likely to encounter exceptions and edge cases, as 
creating branches at certain points can invalidate originally valid prune candidates, 
or prune them in unexpected ways.