package org.skygate.falcon.inventory.rrs;

/**
 * The StorageClassNotIncludedException is thrown when
 * the "fileSchema" in manifest.json file does not include "StorageClass",
 * which means the inventory report is missing the Storage Class information.
 */
public class StorageClassNotIncludedException extends RuntimeException{
    public StorageClassNotIncludedException() {
        super("Storage class NOT found in the inventory report");
    }
}
