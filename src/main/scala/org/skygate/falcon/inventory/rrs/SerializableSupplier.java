package org.skygate.falcon.inventory.rrs;


import java.io.Serializable;
import java.util.function.Supplier;

/**
 * This SerializableSupplier class provides an interface to make a serializable wrapper
 */
public interface SerializableSupplier<T> extends Supplier<T>, Serializable {
}
