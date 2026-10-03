package io.github.green4j.sdsm;

import java.util.Arrays;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * The selector language: text is parsed into a small tree of terms, the tree is read once for
 * what the selector depends on - which keys, which kinds of object, placement, the ends of
 * links - and compiled into the predicate the engine runs. The grammar is in
 * {@code sdsm/README.md}.
 */
final class Selector {

    interface Expression extends Predicate<StructureObject> {

        /**
         * @return whether it reads where objects are placed, so a move between parents can change
         *         what it holds
         */
        default boolean readsPlacement() {
            return false;
        }

        /**
         * @return whether it reads the nodes at the ends of links, so a change of a node can
         *         change which of its links it holds
         */
        default boolean readsEnds() {
            return false;
        }

        /**
         * @return whether it reads the node a port is on, so a change of a node can change which
         *         of its ports it holds
         */
        default boolean readsOwner() {
            return false;
        }

        /**
         * @param keyId a property key
         * @return whether a change of that property, on an object or at the ends of a link, can
         *         change what it holds
         */
        default boolean reads(final int keyId) {
            return true;
        }

        /**
         * @param kind a kind of object
         * @return whether an object of that kind can match at all
         */
        default boolean mayMatch(final ObjectKind kind) {
            return true;
        }
    }

    /**
     * Where an object is placed on an axis.
     */
    @FunctionalInterface
    interface Placement {

        /**
         * @param childId an object
         * @param axis     an axis
         * @return the parent holding it on the axis, or null
         */
        Node parentOn(long childId, String axis);
    }

    /**
     * @param selectorText the selector, empty or null meaning everything
     * @param keys         the naming dictionary of the structure the selector will run against
     * @param placement    where the structure places its objects, for {@code under(...)}
     * @return the compiled expression; it holds key ids, never names, and is single-threaded
     */
    static Expression parse(final String selectorText,
                            final PropertyKeys keys,
                            final Placement placement) {
        if (selectorText == null || selectorText.isBlank()) {
            return new Compiled(candidate -> true, new boolean[0], ALL_KINDS, false, false, false);
        }
        final SelectorParser parser = new SelectorParser(selectorText, keys);
        final Term tree = parser.parseExpression();
        parser.requireEndOfInput();
        final KeySet read = new KeySet();
        tree.collectKeys(read);
        return new Compiled(tree.compile(placement), read.toArray(), tree.kinds(),
                tree.readsPlacement(), tree.readsEnds(), tree.readsOwner());
    }

    private static final Pattern INTEGER =
            Pattern.compile("[+-]?[0-9]+");
    private static final Pattern DECIMAL =
            Pattern.compile("[+-]?[0-9]+(\\.[0-9]+)?([eE][+-]?[0-9]+)?");

    private static final int ALL_KINDS = (1 << ObjectKind.values().length) - 1;

    private static int bitOf(final ObjectKind kind) {
        return 1 << kind.ordinal();
    }

    /**
     * The compiled predicate and what the tree it came from was found to depend on.
     */
    private static final class Compiled implements Expression {

        private final Predicate<StructureObject> body;
        private final boolean[] readKeys;
        private final int kinds;
        private final boolean placement;
        private final boolean ends;
        private final boolean owner;

        Compiled(final Predicate<StructureObject> body,
                 final boolean[] readKeys,
                 final int kinds,
                 final boolean placement,
                 final boolean ends,
                 final boolean owner) {
            this.body = body;
            this.readKeys = readKeys;
            this.kinds = kinds;
            this.placement = placement;
            this.ends = ends;
            this.owner = owner;
        }

        @Override
        public boolean test(final StructureObject candidate) {
            return body.test(candidate);
        }

        @Override
        public boolean readsPlacement() {
            return placement;
        }

        @Override
        public boolean readsEnds() {
            return ends;
        }

        @Override
        public boolean readsOwner() {
            return owner;
        }

        @Override
        public boolean reads(final int keyId) {
            return keyId >= 0 && keyId < readKeys.length && readKeys[keyId];
        }

        @Override
        public boolean mayMatch(final ObjectKind kind) {
            return (kinds & bitOf(kind)) != 0;
        }
    }

    /**
     * The key ids a tree reads.
     */
    private static final class KeySet {
        private boolean[] read = new boolean[16];

        void add(final int keyId) {
            if (keyId >= read.length) {
                read = Arrays.copyOf(read, Math.max(keyId + 1, read.length * 2));
            }
            read[keyId] = true;
        }

        boolean[] toArray() {
            return read;
        }
    }

    /**
     * One term of a parsed selector. It says what it depends on, and compiles itself once into
     * what the engine runs.
     */
    private abstract static class Term {

        abstract Predicate<StructureObject> compile(Placement placement);

        /**
         * @return the kinds of object it can hold, as a mask of {@link ObjectKind} ordinals
         */
        abstract int kinds();

        void collectKeys(final KeySet keys) {
        }

        boolean readsPlacement() {
            return false;
        }

        boolean readsEnds() {
            return false;
        }

        boolean readsOwner() {
            return false;
        }
    }

    private static final class AnyTerm extends Term {
        @Override
        Predicate<StructureObject> compile(final Placement placement) {
            return candidate -> true;
        }

        @Override
        int kinds() {
            return ALL_KINDS;
        }
    }

    private static final class KindTerm extends Term {
        private final ObjectKind kind;

        KindTerm(final ObjectKind kind) {
            this.kind = kind;
        }

        @Override
        Predicate<StructureObject> compile(final Placement placement) {
            final ObjectKind wanted = kind;
            return candidate -> candidate.kind() == wanted;
        }

        @Override
        int kinds() {
            return bitOf(kind);
        }
    }

    private static final class PropertyTerm extends Term {
        private final int keyId;
        private final Operator operator;
        private final String literal;

        PropertyTerm(final int keyId, final Operator operator, final String literal) {
            this.keyId = keyId;
            this.operator = operator;
            this.literal = literal;
        }

        @Override
        Predicate<StructureObject> compile(final Placement placement) {
            return new PropertyPredicate(keyId, operator, literal);
        }

        @Override
        int kinds() {
            return ALL_KINDS;
        }

        @Override
        void collectKeys(final KeySet keys) {
            keys.add(keyId);
        }
    }

    private static final class UnderTerm extends Term {
        private final String axis;
        private final Path path;

        UnderTerm(final String axis, final Path path) {
            this.axis = axis;
            this.path = path;
        }

        @Override
        Predicate<StructureObject> compile(final Placement placement) {
            return new UnderPredicate(axis, path, placement);
        }

        @Override
        int kinds() {
            return ALL_KINDS;
        }

        @Override
        boolean readsPlacement() {
            return true;
        }
    }

    /**
     * The links whose ends' nodes a term holds. What the term reads, it reads on those nodes.
     */
    private static final class EndsTerm extends Term {
        private final Term ends;
        private final boolean both;

        EndsTerm(final Term ends, final boolean both) {
            this.ends = ends;
            this.both = both;
        }

        @Override
        Predicate<StructureObject> compile(final Placement placement) {
            return new EndsPredicate(ends.compile(placement), both);
        }

        @Override
        int kinds() {
            return bitOf(ObjectKind.LINK);
        }

        @Override
        void collectKeys(final KeySet keys) {
            ends.collectKeys(keys);
        }

        @Override
        boolean readsPlacement() {
            return ends.readsPlacement();
        }

        @Override
        boolean readsEnds() {
            return true;
        }
    }

    /**
     * The ports whose nodes a term holds. What the term reads, it reads on those nodes.
     */
    private static final class OnTerm extends Term {
        private final Term owners;

        OnTerm(final Term owners) {
            this.owners = owners;
        }

        @Override
        Predicate<StructureObject> compile(final Placement placement) {
            final Predicate<StructureObject> held = owners.compile(placement);
            return candidate -> candidate instanceof Port && held.test(((Port) candidate).owningNode());
        }

        @Override
        int kinds() {
            return bitOf(ObjectKind.INPUT) | bitOf(ObjectKind.OUTPUT);
        }

        @Override
        void collectKeys(final KeySet keys) {
            owners.collectKeys(keys);
        }

        @Override
        boolean readsPlacement() {
            return owners.readsPlacement();
        }

        @Override
        boolean readsOwner() {
            return true;
        }
    }

    /**
     * What a term does not hold, of every kind: an object whose property is absent is held by
     * {@code !*[key=v]}, never by {@code [key!=v]}.
     */
    private static final class NotTerm extends Term {
        private final Term negated;

        NotTerm(final Term negated) {
            this.negated = negated;
        }

        @Override
        Predicate<StructureObject> compile(final Placement placement) {
            return negated.compile(placement).negate();
        }

        @Override
        int kinds() {
            return ALL_KINDS;
        }

        @Override
        void collectKeys(final KeySet keys) {
            negated.collectKeys(keys);
        }

        @Override
        boolean readsPlacement() {
            return negated.readsPlacement();
        }

        @Override
        boolean readsEnds() {
            return negated.readsEnds();
        }

        @Override
        boolean readsOwner() {
            return negated.readsOwner();
        }
    }

    private static final class AndTerm extends Term {
        private final Term left;
        private final Term right;

        AndTerm(final Term left, final Term right) {
            this.left = left;
            this.right = right;
        }

        @Override
        Predicate<StructureObject> compile(final Placement placement) {
            final Predicate<StructureObject> l = left.compile(placement);
            final Predicate<StructureObject> r = right.compile(placement);
            return candidate -> l.test(candidate) && r.test(candidate);
        }

        @Override
        int kinds() {
            return left.kinds() & right.kinds();
        }

        @Override
        void collectKeys(final KeySet keys) {
            left.collectKeys(keys);
            right.collectKeys(keys);
        }

        @Override
        boolean readsPlacement() {
            return left.readsPlacement() || right.readsPlacement();
        }

        @Override
        boolean readsEnds() {
            return left.readsEnds() || right.readsEnds();
        }

        @Override
        boolean readsOwner() {
            return left.readsOwner() || right.readsOwner();
        }
    }

    private static final class OrTerm extends Term {
        private final Term left;
        private final Term right;

        OrTerm(final Term left, final Term right) {
            this.left = left;
            this.right = right;
        }

        @Override
        Predicate<StructureObject> compile(final Placement placement) {
            final Predicate<StructureObject> l = left.compile(placement);
            final Predicate<StructureObject> r = right.compile(placement);
            return candidate -> l.test(candidate) || r.test(candidate);
        }

        @Override
        int kinds() {
            return left.kinds() | right.kinds();
        }

        @Override
        void collectKeys(final KeySet keys) {
            left.collectKeys(keys);
            right.collectKeys(keys);
        }

        @Override
        boolean readsPlacement() {
            return left.readsPlacement() || right.readsPlacement();
        }

        @Override
        boolean readsEnds() {
            return left.readsEnds() || right.readsEnds();
        }

        @Override
        boolean readsOwner() {
            return left.readsOwner() || right.readsOwner();
        }
    }

    private Selector() {
    }

    private enum Operator {
        PRESENT,
        EQUAL,
        NOT_EQUAL,
        CONTAINS,
        LESS,
        LESS_OR_EQUAL,
        GREATER,
        GREATER_OR_EQUAL;

        boolean orders() {
            return ordinal() >= LESS.ordinal();
        }
    }

    /**
     * What a literal is taken for: a whole number when it is written as one and fits a
     * {@code long}, so it is compared exactly; otherwise a decimal, compared as a {@code double};
     * otherwise text. A whole value never equals a fraction.
     */
    private enum Literal {
        INTEGER,
        DECIMAL,
        TEXT
    }

    /**
     * Compares one property against a literal. The value is read typed and compared typed, so
     * matching a selector costs no allocation - membership is re-evaluated on every property
     * change, which makes this the hottest read in the engine.
     */
    private static final class PropertyPredicate implements Predicate<StructureObject> {

        private final int keyId;
        private final Operator operator;
        private final String expected;
        private final Literal literal;
        private final long expectedLong;
        private final double expectedDouble;
        private final StringBuilder scratch = new StringBuilder(24);

        /**
         * @param keyId    the property
         * @param operator how to compare
         * @param expected the literal, as written; null for {@link Operator#PRESENT}
         */
        PropertyPredicate(final int keyId, final Operator operator, final String expected) {
            this.keyId = keyId;
            this.operator = operator;
            this.expected = expected;
            long asLong = 0L;
            double asDouble = 0.0;
            Literal kind = Literal.TEXT;
            if (expected != null && INTEGER.matcher(expected).matches()) {
                try {
                    asLong = Long.parseLong(expected);
                    asDouble = asLong;
                    kind = Literal.INTEGER;
                } catch (final NumberFormatException tooLarge) {
                    asDouble = Double.parseDouble(expected);
                    kind = Literal.DECIMAL;
                }
            } else if (expected != null && DECIMAL.matcher(expected).matches()) {
                asDouble = Double.parseDouble(expected);
                kind = Literal.DECIMAL;
            }
            if (operator.orders() && kind == Literal.TEXT) {
                throw new IllegalArgumentException(
                        "An ordering compares numbers, and '" + expected + "' is not one");
            }
            this.literal = kind;
            this.expectedLong = asLong;
            this.expectedDouble = asDouble;
        }

        /**
         * A value nobody supplies is not a value: it is unknown, and unknown matches nothing -
         * not even a test for being other than something. A predicate over what a source has
         * been told not to send therefore holds no object, rather than holding all of them.
         *
         * @param candidate the object
         * @return whether it matches
         */
        @Override
        public boolean test(final StructureObject candidate) {
            if (candidate.valueTypeOf(keyId) == ValueType.ABSENT) {
                return false;
            }
            if (operator == Operator.PRESENT) {
                return true;
            }
            final boolean matched = matches(candidate);
            return operator == Operator.NOT_EQUAL ? !matched : matched;
        }

        private boolean matches(final StructureObject candidate) {
            switch (candidate.valueTypeOf(keyId)) {
                case LONG:
                    if (operator == Operator.CONTAINS) {
                        scratch.setLength(0);
                        scratch.append(candidate.longValueOf(keyId));
                        return contains(scratch, expected);
                    }
                    if (operator.orders()) {
                        return ordered(literal == Literal.INTEGER
                                ? Long.compare(candidate.longValueOf(keyId), expectedLong)
                                : compare(candidate.longValueOf(keyId), expectedDouble));
                    }
                    if (literal == Literal.INTEGER) {
                        return candidate.longValueOf(keyId) == expectedLong;
                    }
                    return literal == Literal.DECIMAL && candidate.longValueOf(keyId) == expectedDouble;
                case DOUBLE:
                    if (operator == Operator.CONTAINS) {
                        scratch.setLength(0);
                        scratch.append(candidate.doubleValueOf(keyId));
                        return contains(scratch, expected);
                    }
                    if (operator.orders()) {
                        final double value = candidate.doubleValueOf(keyId);
                        return !Double.isNaN(value) && ordered(compare(value, expectedDouble));
                    }
                    return literal != Literal.TEXT && candidate.doubleValueOf(keyId) == expectedDouble;
                case BOOLEAN:
                    if (operator.orders()) {
                        return false;           // an ordering holds numbers only
                    }
                    return contentsMatch(
                            candidate.booleanValueOf(keyId) ? "true" : "false");
                case TEXT:
                    if (operator.orders()) {
                        return false;
                    }
                    if (keyId == PropertyKeys.ADDRESS && candidate instanceof Port) {
                        return anyAddressMatches((Port) candidate);
                    }
                    return contentsMatch(candidate.textValueOf(keyId));
                default:
                    return false;
            }
        }

        /**
         * As numbers compare, not as {@link Double#compare(double, double)} does, which puts
         * -0.0 below 0.0 while equality holds them the same.
         *
         * @param value    the value, never NaN
         * @param expected the literal
         * @return below zero, zero or above zero
         */
        private static int compare(final double value, final double expected) {
            return value < expected ? -1 : value > expected ? 1 : 0;
        }

        private boolean ordered(final int comparison) {
            switch (operator) {
                case LESS:
                    return comparison < 0;
                case LESS_OR_EQUAL:
                    return comparison <= 0;
                case GREATER:
                    return comparison > 0;
                default:
                    return comparison >= 0;
            }
        }

        /**
         * A port that declares several addresses is compared one address at a time, so
         * {@code address="x"} holds a port that declares x among others, and {@code address!="x"}
         * one that does not declare it.
         *
         * @param port the port
         * @return whether one of its addresses matches
         */
        private boolean anyAddressMatches(final Port port) {
            for (int i = 0; i < port.addressCount(); i++) {
                if (contentsMatch(port.addressAt(i))) {
                    return true;
                }
            }
            return false;
        }

        private boolean contentsMatch(final CharSequence actual) {
            if (operator == Operator.CONTAINS) {
                return contains(actual, expected);
            }
            if (actual.length() != expected.length()) {
                return false;
            }
            for (int i = 0; i < expected.length(); i++) {
                if (actual.charAt(i) != expected.charAt(i)) {
                    return false;
                }
            }
            return true;
        }

        private static boolean contains(final CharSequence actual, final String needle) {
            final int limit = actual.length() - needle.length();
            for (int start = 0; start <= limit; start++) {
                int i = 0;
                while (i < needle.length() && actual.charAt(start + i) == needle.charAt(i)) {
                    i++;
                }
                if (i == needle.length()) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * Holds what lies under the objects a path names on an axis - below them, not they: the
     * object's parents on the axis, from its root, are matched against the path segment by
     * segment. Walking up costs the object's depth and allocates nothing.
     */
    private static final class UnderPredicate implements Predicate<StructureObject> {

        private final String axis;
        private final Path path;
        private final Placement placement;
        private StructureObject[] chain = new StructureObject[8];

        UnderPredicate(final String axis, final Path path, final Placement placement) {
            this.axis = axis;
            this.path = path;
            this.placement = placement;
        }

        @Override
        public boolean test(final StructureObject candidate) {
            int depth = 0;
            Node at = placement.parentOn(candidate.id(), axis);
            while (at != null) {
                if (depth == chain.length) {
                    chain = Arrays.copyOf(chain, depth * 2);
                }
                chain[depth++] = at;
                at = placement.parentOn(at.id(), axis);
            }
            if (depth < path.length()) {
                return false;
            }
            for (int i = 0; i < path.length(); i++) {
                if (!path.matchesAt(i, chain[depth - 1 - i])) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * Holds the links whose ends' nodes an expression holds: both of them, or either.
     */
    private static final class EndsPredicate implements Predicate<StructureObject> {

        private final Predicate<StructureObject> ends;
        private final boolean both;

        EndsPredicate(final Predicate<StructureObject> ends, final boolean both) {
            this.ends = ends;
            this.both = both;
        }

        @Override
        public boolean test(final StructureObject candidate) {
            if (!(candidate instanceof Link)) {
                return false;
            }
            final Link link = (Link) candidate;
            final boolean from = ends.test(link.fromOutput().owningNode());
            return both ? from && ends.test(link.toInput().owningNode())
                    : from || ends.test(link.toInput().owningNode());
        }
    }

    private static final class SelectorParser {

        private static final String UNDER = "under";
        private static final String BETWEEN = "between";
        private static final String TOUCHING = "touching";
        private static final String ON = "on";

        private final String source;
        private final PropertyKeys keys;
        private int position;

        SelectorParser(final String source, final PropertyKeys keys) {
            this.source = source;
            this.keys = keys;
            this.position = 0;
        }

        Term parseExpression() {
            Term leftSide = parseTerm();
            while (peek(',')) {
                consume(',');
                leftSide = new OrTerm(leftSide, parseTerm());
            }
            return leftSide;
        }

        private Term parseTerm() {
            Term leftSide = parseFactor();
            while (peek('&')) {
                consume('&');
                leftSide = new AndTerm(leftSide, parseFactor());
            }
            return leftSide;
        }

        private Term parseFactor() {
            skipWhitespace();
            if (peek('!')) {
                consume('!');
                return new NotTerm(parseFactor());
            }
            if (peek('(')) {
                consume('(');
                final Term nested = parseExpression();
                skipWhitespace();
                consume(')');
                return nested;
            }
            if (peek('[')) {
                return parseProperties(parsePropertyPredicate());
            }

            final String kindLiteral = readIdentifier();
            if (UNDER.equals(kindLiteral) && peek('(')) {
                return parseUnder();
            }
            if ((BETWEEN.equals(kindLiteral) || TOUCHING.equals(kindLiteral)) && peek('(')) {
                consume('(');
                final Term ends = parseExpression();
                skipWhitespace();
                consume(')');
                return new EndsTerm(ends, BETWEEN.equals(kindLiteral));
            }
            if (ON.equals(kindLiteral) && peek('(')) {
                consume('(');
                final Term owners = parseExpression();
                skipWhitespace();
                consume(')');
                return new OnTerm(owners);
            }
            return parseProperties(buildKindTerm(kindLiteral));
        }

        private Term parseProperties(final Term first) {
            Term accumulated = first;
            while (peek('[')) {
                accumulated = new AndTerm(accumulated, parsePropertyPredicate());
            }
            return accumulated;
        }

        private Term parsePropertyPredicate() {
            consume('[');
            skipWhitespace();
            final String propertyKey = readIdentifier();
            skipWhitespace();
            if (peek(']')) {
                consume(']');
                return new PropertyTerm(keys.idOf(propertyKey), Operator.PRESENT, null);
            }
            final Operator operator;
            if (peekTwo("!=")) {
                position += 2;
                operator = Operator.NOT_EQUAL;
            } else if (peekTwo("<=")) {
                position += 2;
                operator = Operator.LESS_OR_EQUAL;
            } else if (peekTwo(">=")) {
                position += 2;
                operator = Operator.GREATER_OR_EQUAL;
            } else if (peek('<')) {
                consume('<');
                operator = Operator.LESS;
            } else if (peek('>')) {
                consume('>');
                operator = Operator.GREATER;
            } else if (peek('~')) {
                consume('~');
                operator = Operator.CONTAINS;
            } else {
                consume('=');
                operator = Operator.EQUAL;
            }
            skipWhitespace();
            final String expectedValue = readValue();
            skipWhitespace();
            consume(']');

            return new PropertyTerm(keys.idOf(propertyKey), operator, expectedValue);
        }

        /**
         * {@code under(<axis>, <path>)}; a {@code )} inside the path is written {@code \)}.
         *
         * @return what lies under the objects the path names on the axis
         */
        private Term parseUnder() {
            consume('(');
            final String axis = readIdentifier();
            consume(',');
            skipWhitespace();
            final StringBuilder text = new StringBuilder();
            while (position < source.length() && source.charAt(position) != ')') {
                if (source.charAt(position) == '\\' && position + 1 < source.length()) {
                    text.append(source.charAt(position++));
                }
                text.append(source.charAt(position++));
            }
            consume(')');
            final Path path = Path.parse(text.toString().trim());
            if (path.length() == 0) {
                throw new IllegalArgumentException("under(...) needs a path at position " + position);
            }
            return new UnderTerm(axis, path);
        }

        private Term buildKindTerm(final String kindLiteral) {
            switch (kindLiteral) {
                case "*":
                    return new AnyTerm();
                case "node":
                    return new KindTerm(ObjectKind.NODE);
                case "link":
                    return new KindTerm(ObjectKind.LINK);
                case "input":
                    return new KindTerm(ObjectKind.INPUT);
                case "output":
                    return new KindTerm(ObjectKind.OUTPUT);
                default:
                    throw new IllegalArgumentException(
                            "Unknown kind '" + kindLiteral + "' at position " + position);
            }
        }

        void requireEndOfInput() {
            skipWhitespace();
            if (position < source.length()) {
                throw new IllegalArgumentException(
                        "Unexpected trailing input at position " + position
                                + ": '" + source.substring(position) + "'");
            }
        }

        private void skipWhitespace() {
            while (position < source.length() && Character.isWhitespace(source.charAt(position))) {
                position++;
            }
        }

        private boolean peek(final char expected) {
            skipWhitespace();
            if (position >= source.length()) {
                return false;
            }
            return source.charAt(position) == expected;
        }

        private boolean peekTwo(final String expected) {
            skipWhitespace();
            return source.startsWith(expected, position);
        }

        private void consume(final char expected) {
            skipWhitespace();
            if (position >= source.length() || source.charAt(position) != expected) {
                throw new IllegalArgumentException(
                        "Expected '" + expected + "' at position " + position);
            }
            position++;
        }

        private String readIdentifier() {
            skipWhitespace();
            if (position < source.length()) {
                final char first = source.charAt(position);
                if (first == '"' || first == '\'') {
                    return readQuotedString(first);
                }
            }
            final int start = position;
            if (position >= source.length()) {
                throw new IllegalArgumentException("Identifier expected at position " + position);
            }
            final char firstChar = source.charAt(position);
            if (firstChar != '*' && !isIdentifierStart(firstChar)) {
                throw new IllegalArgumentException(
                        "Identifier expected at position " + position + " (got '" + firstChar + "')");
            }
            if (firstChar == '*') {
                position++;
                return "*";
            }
            while (position < source.length() && isIdentifierPart(source.charAt(position))) {
                position++;
            }
            return source.substring(start, position);
        }

        private String readValue() {
            skipWhitespace();
            if (position < source.length()) {
                final char first = source.charAt(position);
                if (first == '"' || first == '\'') {
                    return readQuotedString(first);
                }
            }
            final int start = position;
            while (position < source.length()) {
                final char current = source.charAt(position);
                if (current == ']' || Character.isWhitespace(current)) {
                    break;
                }
                if (!isBarewordChar(current)) {
                    break;
                }
                position++;
            }
            if (start == position) {
                throw new IllegalArgumentException("Value expected at position " + position);
            }
            return source.substring(start, position);
        }

        /**
         * Strict quoted-string reader.
         * - Inside a string opened by `quote`, only \\ and \<quote> are valid escapes.
         * - Any other backslash is a syntax error.
         * - The "other" quote is a literal.
         *
         * @param quote opening quote character
         * @return decoded string value
         */
        private String readQuotedString(final char quote) {
            final int openingPosition = position;
            position++; // consume opening quote
            final StringBuilder builder = new StringBuilder();
            while (position < source.length()) {
                final char current = source.charAt(position);
                if (current == quote) {
                    position++;
                    return builder.toString();
                }
                if (current == '\\') {
                    if (position + 1 >= source.length()) {
                        throw new IllegalArgumentException(
                                "Dangling backslash at position " + position);
                    }
                    final char next = source.charAt(position + 1);
                    if (next == '\\' || next == quote) {
                        builder.append(next);
                        position += 2;
                        continue;
                    }
                    throw new IllegalArgumentException(
                            "Invalid escape '\\" + next + "' in "
                                    + (quote == '"' ? "double" : "single")
                                    + "-quoted string at position " + position
                                    + " (only \\\\ and \\" + quote + " are allowed)");
                }
                builder.append(current);
                position++;
            }
            throw new IllegalArgumentException(
                    "Unterminated quoted string starting at position " + openingPosition);
        }

        private static boolean isIdentifierStart(final char c) {
            return Character.isLetter(c) || c == '_';
        }

        private static boolean isIdentifierPart(final char c) {
            return Character.isLetterOrDigit(c) || c == '_' || c == '-' || c == '.';
        }

        private static boolean isBarewordChar(final char c) {
            return Character.isLetterOrDigit(c) || c == '_' || c == '-' || c == '.';
        }
    }
}