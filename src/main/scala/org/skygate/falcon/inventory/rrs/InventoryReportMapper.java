package org.skygate.falcon.inventory.rrs;

import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Iterator;
import java.util.List;

/**
 * This InventoryReportMapper class maps each line of the inventory report to a InventoryReportLine POJO.
 */
public class InventoryReportMapper implements FlatMapFunction<List<String>, InventoryReportLine> {
    private final InventoryManifest manifest;

    public InventoryReportMapper(InventoryManifest inventoryManifest){
        this.manifest = inventoryManifest;
    }

    @Override
    public Iterator<InventoryReportLine> call(List<String> lines) throws Exception {
        InventoryReportLinesMapper mapper = new InventoryReportLinesMapper(manifest);
        return mapper.mapInventoryReportLines(lines).iterator();
    }
}
