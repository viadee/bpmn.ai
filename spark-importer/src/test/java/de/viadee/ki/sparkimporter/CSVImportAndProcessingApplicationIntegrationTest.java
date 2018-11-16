package de.viadee.ki.sparkimporter;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CSVImportAndProcessingApplicationIntegrationTest {

    private static final String TEST_INPUT_FILE_NAME = "./src/test/resources/integration_test_file.csv";

    private static final String TEST_OUTPUT_FILE_PATH = "integration-test-result-csv/";

    private static final String TEST_OUTPUT_FILE_NAME = "integration-test-result-csv/result/csv/result.csv";

    private static final String RESULT_FILE_DELIMITER = "\\|";

    private static String[] headerValues, firstLineValues, secondLineValues, thirdLineValues, fourthLineValues, fifthLineValues, sixthLineValues;

    @BeforeClass
    public static void setUpBeforeClass() throws IOException {
        //System.setProperty("hadoop.home.dir", "C:\\Users\\b60\\Desktop\\hadoop-2.6.0\\hadoop-2.6.0");

        String args[] = {"-fs", TEST_INPUT_FILE_NAME, "-fd", TEST_OUTPUT_FILE_PATH, "-d", ";", "-sr", "false", "-sm", "overwrite", "-of", "csv", "-wd", "./src/test/resources/config/csv/"};
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        SparkSession.builder().config(sparkConf).getOrCreate();

        // run main class
        CSVImportAndProcessingApplication.main(args);

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

    @Ignore
    @Test
    public void testMaxNumberOfRows() {
        assertTrue(sixthLineValues == null);
    }

    @Ignore
    @Test
    public void testColumnHeaders() {
        //check if result contains 41 columns as variable g is filtered out and b renamed to f via the user config
        //+ case_execution_id_ is removed by ColumnRemoveStep
        //+ added jsontest variable with 2 json parameters
        assertEquals(41, headerValues.length);

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
        assertTrue(ArrayUtils.contains(headerValues, "c"));
        assertTrue(ArrayUtils.contains(headerValues, "c_rev"));
        assertTrue(ArrayUtils.contains(headerValues, "d"));
        assertTrue(ArrayUtils.contains(headerValues, "d_rev"));
        assertTrue(ArrayUtils.contains(headerValues, "e"));
        assertTrue(ArrayUtils.contains(headerValues, "e_rev"));
        assertTrue(ArrayUtils.contains(headerValues, "f"));
        assertTrue(ArrayUtils.contains(headerValues, "f_rev"));
        assertTrue(ArrayUtils.contains(headerValues, "i"));
        assertTrue(ArrayUtils.contains(headerValues, "i_rev"));
        assertTrue(ArrayUtils.contains(headerValues, "j"));
        assertTrue(ArrayUtils.contains(headerValues, "j_rev"));

        //check if g and g_rev columns are missing as they are taken out by configuration
        assertTrue(!ArrayUtils.contains(headerValues, "g"));
        assertTrue(!ArrayUtils.contains(headerValues, "g_rev"));

        //check if b b_rev columns are missing as they are renamed to f by configuration
        assertTrue(!ArrayUtils.contains(headerValues, "b"));
        assertTrue(!ArrayUtils.contains(headerValues, "b_rev"));

        //check if case_execution_id_ has been removed by configuration
        assertTrue(!ArrayUtils.contains(headerValues, "case_execution_id_"));

    }


    @Test
    public void testLineValuesHashes() {
        //check if hashes of line values are correct
        //kept in for easier amendment after test case change
//        System.out.println(DigestUtils.md5Hex(Arrays.toString(firstLineValues)).toUpperCase());
//        System.out.println(DigestUtils.md5Hex(Arrays.toString(secondLineValues)).toUpperCase());
//        System.out.println(DigestUtils.md5Hex(Arrays.toString(thirdLineValues)).toUpperCase());
//        System.out.println(DigestUtils.md5Hex(Arrays.toString(fourthLineValues)).toUpperCase());
//        System.out.println(DigestUtils.md5Hex(Arrays.toString(fifthLineValues)).toUpperCase());
    	
    	// TODO - this test fails because of the order of tests
        assertEquals("54A250FFFBC2D61E7D98C68BACB67572", DigestUtils.md5Hex(Arrays.toString(firstLineValues)).toUpperCase());
        assertEquals("17F1FC4994A0097990C5BA2CE44B3045", DigestUtils.md5Hex(Arrays.toString(secondLineValues)).toUpperCase());
        assertEquals("F70F6D20ED7605815A2C0E67E89626E3", DigestUtils.md5Hex(Arrays.toString(thirdLineValues)).toUpperCase());
        assertEquals("FC5C76B8FC93D3F55C9A95832E82F766", DigestUtils.md5Hex(Arrays.toString(fourthLineValues)).toUpperCase());
        assertEquals("C92162E610883DC02E5558CB949F2D4E", DigestUtils.md5Hex(Arrays.toString(fifthLineValues)).toUpperCase());
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
