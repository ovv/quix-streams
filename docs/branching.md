# What is Branching?

When a `StreamingDataFrame` (`SDF`) diverges from a single point into two or more independent 
processing points, we call this `branching`.

## Simple Graph Representation

Assume we have a `StreamingDataFrame`, `sdf` where each letter `X` is
some operation added to it like `sdf = sdf.apply()`.

Before branching, only this was possible:

```
sdf
└── apply()
    └── apply()
        └── apply()
            └── apply()
```

But with branching, you could do something like:

```
sdf
└── apply()
    └── apply()
        ├── apply()
        │   └── apply()
        └── filter() - (does following operations only to this filtered subset)
            ├── apply()
            ├── apply()
            └── apply()
```

## Branching Use Cases

The benefits of branching are fairly apparent, but these are perhaps the most important
aspects:

### Multiple Topic Output

Often, different topics will have different data structures or schemas, which 
necessitates transforming your data before producing to it.

Without branching, it's impossible to handle two or more schemas at once.

### Conditional Operations

While some conditional operations are still achievable within an `.apply()` 
function, now `SDF`'s more natively support it, enabling other `SDF` operations to be more
readily used.

This is especially true for producing to different topics based on different conditions.

### Consolidating Applications

While branching does have some overhead (see [data cloning](#data-cloning)), 
it may enable what once required multiple applications to be consolidated into one,
which is likely to outweigh the cost in many situations.

Consolidating may be useful where many similar transformations are repeated in various
`Applications`, or the context of them overlaps significantly.


## Branching Fundamentals

### Generating Branches

Branches are added almost almost exactly like their linear counterparts 
(which is basically like a branch count of 1).

***The main difference is how variable assignments are managed.***

In short, branches are generated by:

1. adding a new set of operation(s) to an `SDF` instance that you wish to branch from.

    - These operation(s) should _**NOT**_ begin with an [in-place operation](./advanced/dataframe-assignments.md#valid-in-place-operations).

2. the addition should be stored as a **_new variable_**, using _a single, independent assignment step_.

    - Each occurrence of this generates a new branch off the `SDF` instance referenced.

For example (below), `sdf_0` ends up with two branches by adding new
operations to it in two independent steps, stored separately as
`sdf_1` and `sdf_2` (which themselves could then be added to or branched).

```python
sdf_0 = app.dataframe().apply(func_a)
sdf_0 = sdf_0.apply(func_b)  # sdf_0 -> sdf_0: NOT a (new) branch (adds operation)
sdf_1 = sdf_0.apply(func_c)  # sdf_0 -> sdf_1: generates new branch off sdf_0
sdf_2 = sdf_0.apply(func_d)  # sdf_0 -> sdf_2: generates new branch off sdf_0
```

There is _no limit_ to the number of branches you can generate, and branches themselves 
can generate other branches.

### Branching vs. Chaining

Branching is independent of _chaining_, where chaining simply combines multiple 
operations together on a single line like `sdf = sdf.apply().filter().print()`.

Chaining can be used alongside branching; the operations will be collectively treated 
as a single branch addition.

### Branching vs. Multiple Topics (Multi-SDF)

[Consuming multiple topics](consuming-multiple-topics.md) is independent of branching;
branches can be used in each of the `SDF`s from multiple topics, but they cannot
interact with one another in any way.

#### Clarifying Multiple SDFs
`SDF`'s are delineated by the topic they are initialized with;
branches just generate additional nodes you can manipulate on them, 
but they are all still part of the same `SDF` (from the user perspective). 


### Branch Leafs and Tracking

Also being introduced alongside branching is automated `SDF` tracking.

Basically, any `SDF` generated with `Application.dataframe()` will be 
tracked for you, along with any of its branches.

This means `Application.run()` automatically detects all `SDF` (and their branches)
and executes them.

### Branching Limitations

Most of branching's limitations are around:
- [using a branch to manipulate another](#branch-interactions)
- [shared SDF state](#using-with-state)


## Branching Example

In this example, we have purchase events similar to our [purchase-filtering tutorial](tutorials/purchase-filtering/tutorial.md):

In short, customers who have either a `Gold`, `Silver`, or `Bronze` membership
are making purchases at this store.

#### Example Value
```python
kafka_key: "CUSTOMER_ID_123"
kafka_value: {
      "First Name": "Jane",
      "Last Name": "Doe",
      "Email": "jdoe@mail.com",
      "Membership Type": "Gold",
      "Purchases": [
          {
              "Item ID": "abc123",
              "Price": 13.99,
              "Quantity": 12
          },
          {
              "Item ID": "def456",
              "Price": 12.59,
              "Quantity": 2
          },
      ]
  }
```

We want to send coupons to `Silver` or `Gold` members whose purchase total exceeds a 
certain `$` amount.

In addition, _anyone_ who spends at least `$200` becomes eligible to win a car, 
or if they instead spend at least `$1000`, a chance to win **$1 million**.

Basically, something like:

```
Purchases
├── Total >= $200 (prize)
│   ├── Total >= $1000 (cash prize)
│   │   └── produce cash message
│   └── Total < $1000 (car prize)
│       └── produce car message
├── Silver & Total >= $75
│   └── produce coupon message
└── Gold & Total >= $50
    └── produce coupon message
```

### Example Code

```python
from quixstreams import Application

SALES_TAX = 1.10


def get_purchase_totals(items):
    return sum([i["Price"] * i["Quantity"] for i in items])


def message_stub(value):
    return (f'Congratulations {value["First Name"]} {value["Last Name"]} your recent '
            f'purchase totaling {value["Total"]} was enough to earn you')


def coupon(value):
    return f"{message_stub(value)} a coupon!"


def car_prize(value):
    return f"{message_stub(value)} a chance to win a car!"


def cash_prize(value):
    return f"f{message_stub(value)} a chance to win $1 million!"


app = Application("localhost:9092")
customer_purchases = app.topic("customer_purchases", value_deserializer="json")

silver_topic = app.topic("silver_coupon", value_serializer="str")
gold_topic = app.topic("gold_coupon", value_serializer="str")
car_topic = app.topic("car_prize", value_serializer="str")
cash_topic = app.topic("cash_prize", value_serializer="str")

purchases = app.dataframe(customer_purchases)
purchases["Total"] = purchases["Purchases"].apply(get_purchase_totals) * SALES_TAX
purchases.drop(["Email", "Purchases"])

prizes = purchases[purchases["Total"] >= 200.00]
car = prizes[prizes["Total"] < 1000.00].apply(car_prize).to_topic(car_topic)
cash = prizes[prizes["Total"] >= 1000.00].apply(cash_prize).to_topic(cash_topic)

silver_coupon = purchases[
    (purchases["Membership Type"] == "Silver") & (purchases["Total"] >= 75.00)
].apply(coupon).to_topic(silver_topic)

gold_coupon = purchases[
    (purchases["Membership Type"] == "Gold") & (purchases["Total"] >= 50.00)
].apply(coupon).to_topic(gold_topic)

app.run()
```

### Example Processing Result
Processing the [example value](#example-value) would produce 2 values in the following order:

1. `car_prize` topic: 
    - `"Congratulations Jane Doe, your recent purchase totaling $212.36 was enough to earn you a chance to win a car!"`
2. `gold_coupon` topic:
    - `"Congratulations Jane Doe, your recent purchase totaling $212.36 was enough to earn you a coupon!"`


# Execution Ordering

Because of the way branches are tracked and interact, ordering can be tricky.

We make a best effort to execute the SDF **in the order the operations were added**,
or essentially from "top to bottom".

That said, if you have behavior that relies on a specific execution ordering, 
it is recommended to test it and ensure its occurring in the order you expect, as
there may be uncaught edge cases. 

## Ordering Example

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

Interpreted another way, the "last" operation added to each branch occurred in this order.


Here is a visual representation (processed top to bottom):

```
SDF
└── 0 (input)
    └── (+ 10 + 20)
        ├── (+ 70 + 1) = 101
        ├── (+ 102   ) = 132
        └── (+ 500   ) = 530
```

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

## Using with State

While most state functionality works with `branching`, there is one situation to be 
aware of.


### Custom State (`stateful=True`)

Performing `SDF` operations that use `stateful = True` is something to
be careful with, as **stateful operations share the same state between one another**.

For example, assume:

- two branches from the same node: `branch_A` and `branch_B` 
    - each use `.apply(f, stateful = True)`
        - the `f` uses `.set()`/`.get()` on key `my_key`
    - `branch_B` executes after `branch_A`

In this situation, `branch_B`'s `my_key` lookup would be the result of whatever 
`branch_A` did since the state is shared through all SDF branches.

#### Custom state and multi-topic Applications

This "shared" state concern is NOT applicable across the multiple `SDF`'s used for 
multiple topic consuming, as _`SDF` state is tied to its topic name_ 
(among other things).


## Branch Interactions

**Branches should be treated as independent entities that cannot be "combined" 
or "interact".**

The most common ways this is likely to be attempted is filtering and column assigning.

Since there are still valid manipulations with these approaches (corresponding 
to non-branching), it may be tricky to identify valid versus invalid usage.

**If you don't wish to go into too much detail, just take these into consideration**:
 
1. Use the same `SDF` instance as both the source _and_ operation.

    - **Valid**:
        - filtering:
            - `sdf_a = sdf_a[sdf_a.apply(f)]`
            - `sdf_b = sdf_a[sdf_a.apply(f)]` (can branch with filtering!)
        - column assigning:
            - `sdf_a['z'] = sdf_a.apply(f)`
            - `sdf_a['z'] = sdf_a['x'] + sdf_a['y'] + 1`

    - **Invalid**:
        - filtering:
            - `sdf_c = sdf_b[sdf_a.apply(f)]` (`sdf_b` != `sdf_a`)
        - column assigning:
            - `sdf_b['z'] = sdf_a.apply(f)` (`sdf_b` != `sdf_a`)
            - `sdf_c['z'] = sdf_a['x'] + sdf_b['y']` (ALL `sdf` involved must be the SAME)

2. [**AVOID** intermediate operation referencing](./advanced/dataframe-assignments.md#avoid-intermediate-operation-referencing).
    - All the same rules apply with branches.

3. Most common invalid usage _should_ raise exceptions.
 
    - validate results manually if in question


## Performance

Branching does have some performance implications. 

### Data Cloning

Any nodes with branches require cloning the current value at that node `N-1` 
times, where `N` is number of branches at a given node (though any subsequent clones 
are much cheaper relative to the first).

#### Minimizing Performance Loss

Some considerations for mitigating loss in performance due to cloning:

Before (or while) creating a branch:
1. Reduce the value size (ex: use column projection)
    - Smaller value = lower clone cost
2. Filter values upfront
    - lower data volume = less data to clone

#### Cloning Limitations

Data cloning is done using `pickle`, which does mean that it's possible for
the value cloning to fail if the data cannot be serialized with `pickle`.

Though unlikely, if an exception is encountered, try changing the message value to 
a different format before the branching occurs.

## Advanced Usage

### Terminal Branches (no assignment)

Variable assignment is not required to generate branches; skipping assignment generates 
a **terminal branch**.

Terminal branches cannot have any further operations added to them, and can alleviate
the need for instantiating unreferenced variables.

It may still be beneficial to use assignment as a visual aid for those unfamiliar with 
how branching works, and it makes no difference in terms of performance.

#### Rewriting the Example

[The original example code above](#example-code) could be re-written using terminal branches:

```python
from quixstreams import Application

SALES_TAX = 1.10


def get_purchase_totals(items):
    return sum([i["Price"] * i["Quantity"] for i in items])


def message_stub(value):
    return (f'Congratulations {value["First Name"]} {value["Last Name"]} your recent '
            f'purchase totaling {value["Total"]} was enough to earn you')


def coupon(value):
    return f"{message_stub(value)} a coupon!"


def car_prize(value):
    return f"{message_stub(value)} a chance to win a car!"


def cash_prize(value):
    return f"f{message_stub(value)} a chance to win $1 million!"


app = Application("localhost:9092")
customer_purchases = app.topic("customer_purchases", value_deserializer="json")

silver_topic = app.topic("silver_coupon", value_serializer="str")
gold_topic = app.topic("gold_coupon", value_serializer="str")
car_topic = app.topic("car_prize", value_serializer="str")
cash_topic = app.topic("cash_prize", value_serializer="str")

purchases = app.dataframe(customer_purchases)
purchases["Total"] = purchases["Purchases"].apply(get_purchase_totals) * SALES_TAX
purchases.drop(["Email", "Purchases"])


prizes = purchases[purchases["Total"] >= 200.00]
# Removed car and cash assignments
prizes[prizes["Total"] < 1000.00].apply(car_prize).to_topic(car_topic)
prizes[prizes["Total"] >= 1000.00].apply(cash_prize).to_topic(cash_topic)

# Removed silver and gold assignments
purchases[
    (purchases["Membership Type"] == "Silver") & (purchases["Total"] >= 75.00)
].apply(coupon).to_topic(silver_topic)

purchases[
    (purchases["Membership Type"] == "Gold") & (purchases["Total"] >= 50.00)
].apply(coupon).to_topic(gold_topic)

app.run()
```

## Upcoming Features

### Merging

Merging allows you to combine or consolidate branches back into a single processing path.

```
        C ──> D ──> E
       /             \
A ──> B               P ──> Q
       \             /
        K ──> L --->
```

This feature is on the roadmap.