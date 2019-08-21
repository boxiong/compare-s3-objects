package org.skygate.falcon.inventory.rrs;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * This InventoryReportLineMapper class maps the inventory report into InventoryReportLine POJOs
 */
public class InventoryReportLinesMapper implements Serializable {
    private CsvSchema schema;

    public InventoryReportLinesMapper(InventoryManifest inventoryManifest) throws Exception {
        this.schema = CsvSchemaFactory.buildSchema(inventoryManifest);
    }

    /**
     * Map each line of the inventory report into a POJO
     * @return List<InventoryReportLine> which is a list of POJOs
     * @throws IOException when mapping with schema fails
     */
    public List<InventoryReportLine> mapInventoryReportLines(List<String> lines) throws IOException {
        CsvMapper mapper = new CsvMapper();
        List<InventoryReportLine> inventoryReportLines = new ArrayList();

        for (String line : lines) {
            MappingIterator<InventoryReportLine> iterator = mapper.readerFor(InventoryReportLine.class).with(schema).readValues(line);
            List<InventoryReportLine> rows = iterator.readAll();
            if (rows.size() > 0) {
                inventoryReportLines.add(rows.get(0));
            }
        }
        return inventoryReportLines;
    }
}
