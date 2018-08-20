package de.viadee.ki.sparkimporter;

import de.viadee.ki.sparkimporter.processing.PreprocessingRunner;
import de.viadee.ki.sparkimporter.processing.steps.dataprocessing.AddVariablesColumnsStep;
import de.viadee.ki.sparkimporter.processing.steps.dataprocessing.AggregateVariableUpdatesStep;
import de.viadee.ki.sparkimporter.processing.steps.dataprocessing.GetVariablesTypesOccurenceStep;
import de.viadee.ki.sparkimporter.processing.steps.dataprocessing.VariablesTypeEscalationStep;
import de.viadee.ki.sparkimporter.util.SparkBroadcastHelper;
import de.viadee.ki.sparkimporter.util.SparkImporterUtils;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SparkImporterCSVApplicationIntegrationTest {

    private static final String TEST_INPUT_FILE_NAME = "./src/test/resources/integration_test_file.csv";

    private static final String TEST_OUTPUT_FILE_PATH = "integration-test-result/";

    private static final String TEST_OUTPUT_FILE_NAME = "integration-test-result/result.csv";

    private static final String RESULT_FILE_DELIMITER = "\\|";

    private static String[] headerValues, firstLineValues, secondLineValues, thirdLineValues, fourthLineValues, fifthLineValues, sixthLineValues;

    @BeforeClass
    public static void setUpBeforeClass() throws IOException {
        String args[] = {"-fs", TEST_INPUT_FILE_NAME, "-fd", TEST_OUTPUT_FILE_PATH, "-d", ";", "-sr", "false"};
        SparkConf sparkConf = new SparkConf();
        sparkConf.setJars(
                combine(
                        JavaSparkContext.jarOfClass(SparkImporterUtils.class.getClass()),
                        JavaSparkContext.jarOfClass(GetVariablesTypesOccurenceStep.class.getClass()),
                        JavaSparkContext.jarOfClass(VariablesTypeEscalationStep.class.getClass()),
                        JavaSparkContext.jarOfClass(AddVariablesColumnsStep.class.getClass()),
                        JavaSparkContext.jarOfClass(AggregateVariableUpdatesStep.class.getClass()),
                        JavaSparkContext.jarOfClass(SparkBroadcastHelper.class.getClass()),
                        JavaSparkContext.jarOfClass(SparkBroadcastHelper.BROADCAST_VARIABLE.class.getClass()),
                        JavaSparkContext.jarOfClass(PreprocessingRunner.class.getClass())
                )
        );
        sparkConf.setMaster("local[1]");
        SparkSession.builder().config(sparkConf).getOrCreate();

        // run main class
        SparkImporterCSVApplication.main(args);

        //read result csv
        BufferedReader resultFileReader = new BufferedReader(new FileReader(new File(TEST_OUTPUT_FILE_NAME)));

        headerValues = resultFileReader.readLine().split(RESULT_FILE_DELIMITER);
        firstLineValues = resultFileReader.readLine().split(RESULT_FILE_DELIMITER);
        secondLineValues = resultFileReader.readLine().split(RESULT_FILE_DELIMITER);
        thirdLineValues = resultFileReader.readLine().split(RESULT_FILE_DELIMITER);
        fourthLineValues = resultFileReader.readLine().split(RESULT_FILE_DELIMITER);
        fifthLineValues = resultFileReader.readLine().split(RESULT_FILE_DELIMITER);

        //result should only contain 5 value lines
        try {
            sixthLineValues = resultFileReader.readLine().split(RESULT_FILE_DELIMITER);
        } catch (NullPointerException e) {
            //expected, so continue. will be tested later
        }

        resultFileReader.close();
    }

    @Test
    public void testMaxNumberOfRows() throws IOException {
        assertTrue(sixthLineValues == null);
    }

    @Test
    public void testColumnHeaders() throws IOException {
        //check if result contains 38 columns
        assertTrue(headerValues.length == 38);

        //check if the following columns exist
        assertTrue(ArrayUtils.contains(headerValues, "id_"));
        assertTrue(ArrayUtils.contains(headerValues, "proc_inst_id_"));
        assertTrue(ArrayUtils.contains(headerValues, "business_key_"));
        assertTrue(ArrayUtils.contains(headerValues, "proc_def_key_"));
        assertTrue(ArrayUtils.contains(headerValues, "proc_def_id_"));
        assertTrue(ArrayUtils.contains(headerValues, "start_time_"));
        assertTrue(ArrayUtils.contains(headerValues, "end_time_"));
        assertTrue(ArrayUtils.contains(headerValues, "duration_"));
        assertTrue(ArrayUtils.contains(headerValues, "start_user_id_"));
        assertTrue(ArrayUtils.contains(headerValues, "start_act_id_"));
        assertTrue(ArrayUtils.contains(headerValues, "end_act_id_"));
        assertTrue(ArrayUtils.contains(headerValues, "super_process_instance_id_"));
        assertTrue(ArrayUtils.contains(headerValues, "super_case_instance_id_"));
        assertTrue(ArrayUtils.contains(headerValues, "delete_reason_"));
        assertTrue(ArrayUtils.contains(headerValues, "tenant_id_"));
        assertTrue(ArrayUtils.contains(headerValues, "state_"));
        assertTrue(ArrayUtils.contains(headerValues, "execution_id_"));
        assertTrue(ArrayUtils.contains(headerValues, "act_inst_id_"));
        assertTrue(ArrayUtils.contains(headerValues, "case_def_key_"));
        assertTrue(ArrayUtils.contains(headerValues, "case_def_id_"));
        assertTrue(ArrayUtils.contains(headerValues, "case_inst_id_"));
        assertTrue(ArrayUtils.contains(headerValues, "case_execution_id_"));
        assertTrue(ArrayUtils.contains(headerValues, "task_id_"));
        assertTrue(ArrayUtils.contains(headerValues, "bytearray_id_"));

        //check if the following columns don't exist anymore
        assertTrue(!ArrayUtils.contains(headerValues, "name_"));
        assertTrue(!ArrayUtils.contains(headerValues, "var_type_"));
        assertTrue(!ArrayUtils.contains(headerValues, "rev_"));
        assertTrue(!ArrayUtils.contains(headerValues, "double_"));
        assertTrue(!ArrayUtils.contains(headerValues, "long_"));
        assertTrue(!ArrayUtils.contains(headerValues, "text_"));
        assertTrue(!ArrayUtils.contains(headerValues, "text2_"));

        //check if new variable columns and their _rev columns exist
        assertTrue(ArrayUtils.contains(headerValues, "a"));
        assertTrue(ArrayUtils.contains(headerValues, "a_rev"));
        assertTrue(ArrayUtils.contains(headerValues, "b"));
        assertTrue(ArrayUtils.contains(headerValues, "b_rev"));
        assertTrue(ArrayUtils.contains(headerValues, "c"));
        assertTrue(ArrayUtils.contains(headerValues, "c_rev"));
        assertTrue(ArrayUtils.contains(headerValues, "d"));
        assertTrue(ArrayUtils.contains(headerValues, "d_rev"));
        assertTrue(ArrayUtils.contains(headerValues, "e"));
        assertTrue(ArrayUtils.contains(headerValues, "e_rev"));
        assertTrue(ArrayUtils.contains(headerValues, "f"));
        assertTrue(ArrayUtils.contains(headerValues, "f_rev"));
        assertTrue(ArrayUtils.contains(headerValues, "g"));
        assertTrue(ArrayUtils.contains(headerValues, "g_rev"));
    }

    @Test
    public void testLineValuesHashes() {
        //check if hashes of line values are correct
        System.out.println(DigestUtils.md5Hex(Arrays.toString(firstLineValues)).toUpperCase());
        System.out.println(DigestUtils.md5Hex(Arrays.toString(secondLineValues)).toUpperCase());
        System.out.println(DigestUtils.md5Hex(Arrays.toString(thirdLineValues)).toUpperCase());
        System.out.println(DigestUtils.md5Hex(Arrays.toString(fourthLineValues)).toUpperCase());
        System.out.println(DigestUtils.md5Hex(Arrays.toString(fifthLineValues)).toUpperCase());
        assertEquals(DigestUtils.md5Hex(Arrays.toString(firstLineValues)).toUpperCase(), "5E0E0F6757CD494C79350D65D02A76E3");
        assertEquals(DigestUtils.md5Hex(Arrays.toString(secondLineValues)).toUpperCase(), "BF81F2A31E15ECB35164ABAE306411C8");
        assertEquals(DigestUtils.md5Hex(Arrays.toString(thirdLineValues)).toUpperCase(), "167BE6C759696772C15B1E193D378E02");
        assertEquals(DigestUtils.md5Hex(Arrays.toString(fourthLineValues)).toUpperCase(), "5F89D623F24B334B943281F4E7CD080B");
        assertEquals(DigestUtils.md5Hex(Arrays.toString(fifthLineValues)).toUpperCase(), "C0134C1E1C328C5A8DF6EC63E88FEA81");
    }

    private static String[] combine(String[] a, String[]... b){
        int length = a.length;
        for(String[] b2 : b) {
            length += b2.length;
        }
        String[] result = new String[length];
        int startPos = a.length;
        System.arraycopy(a, 0, result, 0, a.length);
        for(String[] b2 : b) {
            length += b2.length;
            System.arraycopy(b2, 0, result, startPos, b2.length);
            startPos += b2.length;
        }
        return result;
    }
}
