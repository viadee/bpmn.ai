package de.viadee.ki.sparkimporter.processing.aggregation;

import de.viadee.ki.sparkimporter.util.SparkImporterVariables;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class ProcessStatesAggregationFunction extends UserDefinedAggregateFunction {

    private StructType inputSchema;
    private StructType bufferSchema;

    public ProcessStatesAggregationFunction() {
        inputSchema = DataTypes.createStructType(new StructField[]{DataTypes.createStructField("value", DataTypes.StringType, true)});
        bufferSchema = DataTypes.createStructType(new StructField[]{DataTypes.createStructField("currentSelection", DataTypes.StringType, true)});
    }

    // Data types of input arguments of this aggregate function
    @Override
    public StructType inputSchema() {
        return inputSchema;
    }

    // Data types of values in the aggregation buffer
    @Override
    public StructType bufferSchema() {
        return bufferSchema;
    }

    // The data type of the returned value
    @Override
    public DataType dataType() {
        return DataTypes.StringType;
    }

    // Whether this function always returns the same output on the identical input
    @Override
    public boolean deterministic() {
        return true;
    }

    // Initializes the given aggregation buffer. The buffer itself is a `Row` that in addition to
    // standard methods like retrieving a value at an index (e.g., get(), getBoolean()), provides
    // the opportunity to update its values. Note that arrays and maps inside the buffer are still
    // immutable.
    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0, SparkImporterVariables.PROCESS_STATE_ACTIVE);
    }

    // Updates the given aggregation buffer `buffer` with new input data from `input`
    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        //TODO: only implemented for ACTIVE and COMPLETED state so far
        if (!input.isNullAt(0)) {
            String currentValue = (buffer.size() == 0 || buffer.getString(0) == null ? SparkImporterVariables.PROCESS_STATE_ACTIVE : buffer.getString(0));

            String value = currentValue;
            if(!currentValue.equals(SparkImporterVariables.PROCESS_STATE_COMPLETED)){
                if(input.getString(0).equals(SparkImporterVariables.PROCESS_STATE_COMPLETED)) {
                    buffer.update(0, SparkImporterVariables.PROCESS_STATE_COMPLETED);
                }
            }
        }
    }

    // Merges two aggregation buffers and stores the updated buffer values back to `buffer1`
    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        //TODO: only implemented for ACTIVE and COMPLETED state so far
        String value = SparkImporterVariables.PROCESS_STATE_ACTIVE;
        if (!buffer1.isNullAt(0) && !buffer2.isNullAt(0)) {
            String b1 = buffer1.getString(0);
            String b2 = buffer2.getString(0);

            if(b1.equals(SparkImporterVariables.PROCESS_STATE_COMPLETED)){
                value = b1;
            } else {
                if(b2.equals(SparkImporterVariables.PROCESS_STATE_COMPLETED)) {
                    value = SparkImporterVariables.PROCESS_STATE_COMPLETED;
                }
            }
        } else if(!buffer1.isNullAt(0)){
            value = buffer2.getString(0);
        } else {
            value = buffer1.getString(0);
        }
        buffer1.update(0, value);
    }

    // Calculates the final result
    @Override
    public String evaluate(Row buffer) {
        return buffer.getString(0);
    }
}