package org.apache.doris.analysis.multi.routineload;

import org.apache.doris.common.AnalysisException;

import java.util.List;
import java.util.Map;

public abstract class MultiRoutineLoadDataSourceProperties {

    protected String datasourceType;

    protected Map<String,Object> datasourceProperties;

    protected boolean isAlter;


    public MultiRoutineLoadDataSourceProperties( Map<String, Object> properties, boolean isAlter) throws AnalysisException {
        this.isAlter = isAlter;
        this.datasourceProperties = properties;/**/
        checkPropertyExist(requiredProperties());
    }

    public abstract boolean checkParameters();

    public abstract String[] requiredProperties();

    protected void checkPropertyExist(String... propertyName) throws AnalysisException {
        for (String name : propertyName) {
            if (!datasourceProperties.containsKey(name)) {
                throw new AnalysisException("Routine load data source property: " + name + " must be set");
            }
        }
    }
}
