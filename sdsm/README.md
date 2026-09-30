## Selector Language Specification

### Purpose

A selector picks objects from a `Structure`. It defines what a view holds and what `query` and
`matchedObjectIds` return, and it is kept up to date as the objects it reads change. It combines:

- a **kind** filter: `node`, `link`, `input`, `output`, `*`;
- **property predicates**: `[zone=eu]`, `[cpu>80]`;
- **placement**: `under(placement, /eu-de/*)`, what lies below a path on an axis;
- **the ends of links**: `between(e)` and `touching(e)`, links whose nodes `e` selects;
- boolean composition: `&` AND, `,` OR, parentheses.

It is minimal, side-effect-free and parsed in one pass. An empty selector selects everything.

A selector is parsed into a tree of terms, read once for what it depends on - which keys, which
kinds, placement, the ends of links - and compiled into the predicate the engine runs. A write
to a property a view's selector does not read never re-evaluates that view.

### EBNF Grammar

```ebnf
selector              = [ expression ] ;                          (* empty: everything *)

expression            = term , { "," , term } ;                   (* OR *)
term                  = factor , { "&" , factor } ;               (* AND *)
factor                = "(" , expression , ")"
                      | under
                      | ends
                      | kindLiteral , { propertyPredicate }
                      | propertyPredicate , { propertyPredicate } ;

kindLiteral           = "*" | "node" | "link" | "input" | "output" ;

under                 = "under" , "(" , axis , "," , path , ")" ;
axis                  = identifier ;
path                  = ? a Path, as Path.parse reads it; a ")" inside is written "\)" ? ;

ends                  = ( "between" | "touching" ) , "(" , expression , ")" ;

propertyPredicate     = "[" , propertyKey , operator , propertyValue , "]" ;
propertyKey           = identifier ;
operator              = "=" | "!=" | "~" | "<" | "<=" | ">" | ">=" ;
propertyValue         = quotedString | bareword ;

identifier            = bareIdentifier | quotedString ;
bareIdentifier        = identifierStart , { identifierPart } ;
identifierStart       = letter | "_" ;
identifierPart        = letter | digit | "_" | "-" | "." ;

bareword              = barewordChar , { barewordChar } ;
barewordChar          = letter | digit | "_" | "-" | "." ;

quotedString          = singleQuotedString | doubleQuotedString ;
singleQuotedString    = "'" , { sqEscape | sqLiteral } , "'" ;
sqEscape              = "\\" , ( "'" | "\\" ) ;
sqLiteral             = ? any character except ' and \ ? ;
doubleQuotedString    = '"' , { dqEscape | dqLiteral } , '"' ;
dqEscape              = "\\" , ( '"' | "\\" ) ;
dqLiteral             = ? any character except " and \ ? ;

letter                = ? a Unicode letter ? ;
digit                 = ? a Unicode digit ? ;

(* whitespace is allowed between any two tokens *)
```

`under`, `between` and `touching` are keywords only when a `(` follows them.

### Semantics

#### Kinds

| Literal  | Matches `ObjectKind` |
|----------|----------------------|
| `*`      | any                  |
| `node`   | `NODE`               |
| `link`   | `LINK`               |
| `input`  | `INPUT`              |
| `output` | `OUTPUT`             |

`node[a=1][b=2]` is `node & [a=1] & [b=2]`, and `[a=1]` alone is `* & [a=1]`.

#### Property predicates

`[key OP value]` reads the property `key` of the object. The key is any property, including the
intrinsic ones every object answers from its own fields - listed in the
[top-level README](../README.md#9-reserved-properties-and-stable-identity).

**An absent property matches nothing** - under every operator, `!=` included. `[cpu!=0]` holds no
object whose `cpu` is not there: a value nobody supplies is unknown, not different.

A literal is read by how it is written, whether quoted or not:

- a **whole number** - `42`, `-7` - that fits a `long`;
- a **decimal** - `1.5`, `2e3` - otherwise;
- **text**, anything else. `1d` and `0x10` are text.

| Operator | Meaning |
|---|---|
| `=` | equal. A whole value equals a whole literal exactly, and never a fraction: `[replicas=1.5]` does not hold `1`, `[replicas=1.0]` does. A decimal value is compared as a `double`. Text and `true` / `false` are compared as text. |
| `!=` | not `=`, for a present value |
| `~` | the value, as text, contains the literal; case-sensitive |
| `<` `<=` `>` `>=` | numeric order. The literal must be a number, or the selector is refused; a value that is not a number - text, a boolean, `NaN` - holds none of them. |

#### Placement

`under(axis, path)` holds what lies **below** the objects the path names on the axis - not those
objects themselves. The object's parents on the axis, from its root, are matched segment by
segment: a segment is `name`, `type:name`, `#id`, `*` or `type:*`, and `/ : # * \` inside a name
are written after `\`.

```
node & under(placement, /eu-de/*)                 -- nodes below any child of eu-de
under(placement, /region:eu-de/silo:*/tradingday:*)
```

The selection follows moves: a node placed under another parent, alone or with what it holds,
enters or leaves at once. The path must name at least one segment.

#### The ends of links

- `between(e)` holds the links both of whose ends' nodes `e` holds;
- `touching(e)` holds the links at least one of whose ends' nodes `e` holds.

```
node[type=service], between(node[type=service])   -- services and the links among them
node[name=ingest], touching(node[name=ingest])    -- one node and every link at it
```

They are kept up to date as the nodes change. A view holding a link also delivers the two ports
it joins.

#### Composition

- `A & B` - both hold.
- `A , B` - either holds.
- `(...)` - grouping.
- `&` binds tighter than `,`: `a & b , c & d` is `(a & b) , (c & d)`.

#### Quoting and escapes

A key or a value that is not a bare identifier or bareword - spaces, brackets, operators, quotes,
`:`, `/`, `+` - is quoted. Inside a quoted string exactly two escapes are recognised: the quote
that opened it, and `\\`.

| Source    | Decoded          |
|-----------|------------------|
| `"a\"b"`  | `a"b`            |
| `'a\'b'`  | `a'b`            |
| `"a\\b"`  | `a\b`            |
| `"a'b"`   | `a'b` (literal)  |
| `"a\nb"`  | **syntax error** |

Any other backslash is a syntax error.

#### Errors

A selector that does not parse is refused with an `IllegalArgumentException` saying why, and where
when a position applies:
an unknown kind, an unterminated string, an invalid escape, trailing input, an ordering against a
literal that is not a number, `under` without a path.

### Examples

```
*                                               -- everything
node                                            -- all nodes
node[zone=eu] & [status!=DOWN]                  -- nodes in eu that are not DOWN
node[zone=eu] , link[type=tcp]                  -- eu nodes, or tcp links
(node , link) & [owner~"team-"]                 -- nodes or links whose owner contains "team-"
node[cpu>80] & [load<=0.9]                      -- numeric order
input[address="tb:trades.eu"]                   -- who is waiting for an address
node & under(placement, /eu-de/blue)            -- nodes below the blue silo of eu-de
node[type=service], between(node[type=service]) -- services and the links among them
*["weird key with spaces" = "v"]                -- quoted key
link[description = 'It\'s ok']                  -- escaped quote
```
