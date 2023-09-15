package edu.berkeley.cs186.database.concurrency;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

/**
 * Utility methods to track the relationships between different lock types.
 */
public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        HashMap<LockType, ArrayList<LockType>> compatible = new HashMap<>();
        compatible.put(S, new ArrayList<>(Arrays.asList(S, IS, NL)));
        compatible.put(X, new ArrayList<>(Arrays.asList(NL)));
        compatible.put(IS, new ArrayList<>(Arrays.asList(S, IS, IX, SIX, NL)));
        compatible.put(IX, new ArrayList<>(Arrays.asList(IS, IX, NL)));
        compatible.put(SIX, new ArrayList<>(Arrays.asList(IS, NL)));
        compatible.put(NL, new ArrayList<>(Arrays.asList(S, X, IS, IX, SIX, NL)));

        return compatible.get(a).contains(b);
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        HashMap<LockType, ArrayList<LockType>> grantedChildren = new HashMap<>();
        grantedChildren.put(S, new ArrayList<>(Arrays.asList(S, IS, NL)));
        grantedChildren.put(X, new ArrayList<>(Arrays.asList(NL)));
        grantedChildren.put(IS, new ArrayList<>(Arrays.asList(S, IS, NL)));
        grantedChildren.put(IX, new ArrayList<>(Arrays.asList(S, X, IS, IX, SIX, NL)));
        grantedChildren.put(SIX, new ArrayList<>(Arrays.asList(X, IX, NL)));
        grantedChildren.put(NL, new ArrayList<>(Arrays.asList(NL)));

        return grantedChildren.get(parentLockType).contains(childLockType);
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        // can `substitute` substitute `required`?
        HashMap<LockType, ArrayList<LockType>> substitutes = new HashMap<>();
        substitutes.put(S, new ArrayList<>(Arrays.asList(S, X, SIX)));
        substitutes.put(X, new ArrayList<>(Arrays.asList(X)));
        substitutes.put(IS, new ArrayList<>(Arrays.asList(IS, IX)));
        substitutes.put(IX, new ArrayList<>(Arrays.asList(IX, SIX)));
        substitutes.put(SIX, new ArrayList<>(Arrays.asList(S, IX, SIX)));
        substitutes.put(NL, new ArrayList<>(Arrays.asList(NL)));

        return substitutes.get(required).contains(substitute);
    }

    /**
     * @return True if this lock is IX, IS, or SIX. False otherwise.
     */
    public boolean isIntent() {
        return this == LockType.IX || this == LockType.IS || this == LockType.SIX;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

