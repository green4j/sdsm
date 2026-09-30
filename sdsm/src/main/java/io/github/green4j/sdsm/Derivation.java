package io.github.green4j.sdsm;

/**
 * A property the structure works out rather than being told: the fold of properties of the
 * object itself, of a property of its children, or of both. Over children it also says how
 * many of the summands are known and how many there are. A child is a summand in its own
 * right, or, if it folds the same key over its children the same way, brings in its own
 * summands - so a cluster's "3 of 4" counts streams, not releases. A fold that also reads the
 * child's own keys is a summand with its value, as what a component says of itself is one
 * thing, however many sources said it.
 * <p>
 * The value is over the known summands, and is absent only when none is known: a partial sum
 * is readable next to its count, and an object nobody is paying for says nothing.
 */
final class Derivation {

    final int keyId;
    final int knownKeyId;
    final int totalKeyId;
    final Fold fold;
    final Over over;
    /** Where to read off a member; null unless over members. */
    final MemberPath memberPath;

    /** What the summands come to now. */
    final Tally tally;

    /** What the object and the parents above it were last told. */
    final Tally published;

    /** What they were told before that, while the parents take it out. */
    final Tally previous;

    /** What each member brings in now, so a member's change replaces its part; null unless over members. */
    final LongObjectMap<Sum> contributions;

    Derivation(final int keyId,
               final int knownKeyId,
               final int totalKeyId,
               final Fold fold,
               final Over over,
               final MemberPath memberPath) {
        this.keyId = keyId;
        this.knownKeyId = knownKeyId;
        this.totalKeyId = totalKeyId;
        this.fold = fold;
        this.over = over;
        this.memberPath = memberPath;
        this.tally = Tally.of(fold);
        this.published = Tally.of(fold);
        this.previous = Tally.of(fold);
        this.contributions = memberPath != null ? new LongObjectMap<>() : null;
    }

    /**
     * @param below what a child derives
     * @return whether that child brings in its summands here: it folds the key read over its
     *         children alone, the same way
     */
    boolean foldsOver(final Derivation below) {
        return over.childKeyId == below.keyId && fold == below.fold && below.over.ownKeyIds.length == 0;
    }

    boolean sameAs(final Fold otherFold, final Over otherOver) {
        return fold == otherFold && over.equals(otherOver);
    }

    /**
     * @param child a child of the object
     * @return whether it is a summand: the fold is over children, of the type given or any
     */
    boolean takes(final StructureObject child) {
        return over.overChildren() && (over.childType == null || over.childType.equals(child.type()));
    }

    boolean derives(final int key) {
        return key == keyId || over.counts() && (key == knownKeyId || key == totalKeyId);
    }

    /**
     * What summands come to, and how many are known of how many there are. Whole numbers and
     * fractions are kept apart, so a sum of whole numbers stays one.
     */
    abstract static class Tally {
        long longs;
        int doubleCount;
        int known;
        int total;

        static Tally of(final Fold fold) {
            return fold == Fold.SUM ? new Sum() : new Extreme(fold == Fold.MAX);
        }

        /**
         * @return what the fractions come to
         */
        abstract double doubles();

        /**
         * @return the value, when any summand is a fraction
         */
        abstract double doubleValue();

        abstract void add(Tally other);

        /**
         * Takes one summand's part out and puts another in, if this tally can.
         *
         * @param was what the summand came to
         * @param now what it comes to now
         * @return false if the summands have to be recounted instead
         */
        abstract boolean replace(Tally was, Tally now);

        abstract void set(Tally other);

        /**
         * @param other a tally of the same fold
         * @return whether the two come to the same value and counts
         */
        boolean same(final Tally other) {
            return longs == other.longs && doubleCount == other.doubleCount
                    && known == other.known && total == other.total
                    && Double.doubleToLongBits(doubles()) == Double.doubleToLongBits(other.doubles());
        }

        void clear() {
            longs = 0L;
            doubleCount = 0;
            known = 0;
            total = 0;
        }

        void unknown() {
            clear();
            total = 1;
        }

        void copyCounts(final Tally other) {
            longs = other.longs;
            doubleCount = other.doubleCount;
            known = other.known;
            total = other.total;
        }
    }

    /**
     * A sum kept by adding and taking away. Whole numbers wrap and unwrap exactly. Fractions are
     * added with a compensation for what rounding lost, so a large summand that has gone does not
     * take a small one with it; and a summand that is not a finite number is counted apart, so
     * the sum is a number again once it has gone.
     */
    static final class Sum extends Tally {
        private double finite;
        private double compensation;
        private int notNumbers;
        private int positiveInfinities;
        private int negativeInfinities;

        /**
         * Makes this the one summand an object's property is.
         *
         * @param member the object
         * @param keyId  the property
         */
        void ofLeaf(final StructureObject member, final int keyId) {
            unknown();
            switch (member.valueTypeOf(keyId)) {
                case LONG:
                    longs = member.longValueOf(keyId);
                    known = 1;
                    break;
                case DOUBLE:
                    addDouble(member.doubleValueOf(keyId));
                    doubleCount = 1;
                    known = 1;
                    break;
                default:
                    break;
            }
        }

        /**
         * Makes this the one summand a derived property is, as the structure writes it from a
         * tally: nothing known is no value, a fraction among the summands makes it a fraction.
         *
         * @param tally what the property was written from
         */
        void ofTally(final Tally tally) {
            unknown();
            if (tally.known == 0) {
                return;
            }
            if (tally.doubleCount > 0) {
                addDouble(tally.doubleValue());
                doubleCount = 1;
            } else {
                longs = tally.longs;
            }
            known = 1;
        }

        @Override
        double doubles() {
            if (notNumbers > 0 || positiveInfinities > 0 && negativeInfinities > 0) {
                return Double.NaN;
            }
            if (positiveInfinities > 0) {
                return Double.POSITIVE_INFINITY;
            }
            if (negativeInfinities > 0) {
                return Double.NEGATIVE_INFINITY;
            }
            return Double.isFinite(finite) ? finite + compensation : finite;
        }

        @Override
        double doubleValue() {
            return longs + doubles();
        }

        @Override
        void add(final Tally other) {
            combine((Sum) other, 1);
        }

        @Override
        boolean replace(final Tally was, final Tally now) {
            combine((Sum) was, -1);
            combine((Sum) now, 1);
            return Double.isFinite(finite);   // a finite sum that overflowed cannot be taken back
        }

        private void combine(final Sum other, final int sign) {
            longs += sign * other.longs;
            doubleCount += sign * other.doubleCount;
            known += sign * other.known;
            total += sign * other.total;
            notNumbers += sign * other.notNumbers;
            positiveInfinities += sign * other.positiveInfinities;
            negativeInfinities += sign * other.negativeInfinities;
            if (doubleCount == 0) {
                finite = 0.0;           // the fractions start again from nothing
                compensation = 0.0;
                return;
            }
            addFinite(sign * other.finite);
            addFinite(sign * other.compensation);
        }

        private void addDouble(final double value) {
            if (Double.isNaN(value)) {
                notNumbers++;
            } else if (value == Double.POSITIVE_INFINITY) {
                positiveInfinities++;
            } else if (value == Double.NEGATIVE_INFINITY) {
                negativeInfinities++;
            } else {
                addFinite(value);
            }
        }

        /**
         * Neumaier's summation: what rounding drops from the running sum is kept beside it.
         *
         * @param value a finite summand
         */
        private void addFinite(final double value) {
            final double sum = finite + value;
            if (!Double.isFinite(sum)) {
                finite = sum;           // overflowed: nothing to compensate, and recounted on change
                return;
            }
            compensation += Math.abs(finite) >= Math.abs(value)
                    ? (finite - sum) + value
                    : (value - sum) + finite;
            finite = sum;
        }

        @Override
        void set(final Tally other) {
            final Sum sum = (Sum) other;
            copyCounts(sum);
            finite = sum.finite;
            compensation = sum.compensation;
            notNumbers = sum.notNumbers;
            positiveInfinities = sum.positiveInfinities;
            negativeInfinities = sum.negativeInfinities;
        }

        @Override
        void clear() {
            super.clear();
            finite = 0.0;
            compensation = 0.0;
            notNumbers = 0;
            positiveInfinities = 0;
            negativeInfinities = 0;
        }
    }

    /**
     * The largest or the smallest summand. It cannot take one back, so a change recounts.
     */
    static final class Extreme extends Tally {
        private final boolean max;
        private double doubles;

        Extreme(final boolean max) {
            this.max = max;
        }

        @Override
        double doubles() {
            return doubles;
        }

        @Override
        double doubleValue() {
            if (known == doubleCount) {
                return doubles;
            }
            return max ? Math.max(longs, doubles) : Math.min(longs, doubles);
        }

        @Override
        void add(final Tally other) {
            if (other.known > other.doubleCount) {
                longs = known == doubleCount ? other.longs
                        : max ? Math.max(longs, other.longs) : Math.min(longs, other.longs);
            }
            if (other.doubleCount > 0) {
                final double value = other.doubles();
                doubles = doubleCount == 0 ? value
                        : max ? Math.max(doubles, value) : Math.min(doubles, value);
            }
            doubleCount += other.doubleCount;
            known += other.known;
            total += other.total;
        }

        @Override
        boolean replace(final Tally was, final Tally now) {
            return false;
        }

        @Override
        void set(final Tally other) {
            copyCounts(other);
            doubles = ((Extreme) other).doubles;
        }

        @Override
        void clear() {
            super.clear();
            doubles = 0.0;
        }
    }
}
