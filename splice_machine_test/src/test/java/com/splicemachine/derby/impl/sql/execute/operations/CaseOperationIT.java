package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.*;

import java.sql.ResultSet;

/**
 * Created by akorotenko on 12/11/15.
 */
public class CaseOperationIT {

    private static final String SCHEMA = CaseOperationIT.class.getSimpleName();
    private static SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void setup() throws Exception {
        classWatcher.executeUpdate(" create table warehouse_schema_regimens (id int not null primary key, name varchar(20))");
        classWatcher.executeUpdate(" create table warehouse_schema_decisions (\n" +
                "        id int not null primary key,\n" +
                "        decided_on_year int,\n" +
                "        decided_on_month int,\n" +
                "        custom_name varchar(20),\n" +
                "        regimen_id int)");

        classWatcher.executeUpdate("insert into warehouse_schema_regimens values (1, 'regimens 1'), (2, 'regimens 2'), (3, 'regimens 3')");

        classWatcher.executeUpdate("insert into warehouse_schema_decisions values (1, 2014, 8, 'name 1', 1), " +
                "(2, 2014, 9, 'name 1', 1), (3, 2015, 10, 'name 2', 1), (4, 2015, 11, 'name 3', 2), (5, 2015, 1, null, 2)");
    }

    /**
     * DB-3867 add test coverage: estimating cost when group aggregate has case statement
     */
    @Test
    public void test() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("\n" +
                "SELECT COUNT(warehouse_schema_decisions.id) AS count, warehouse_schema_decisions.decided_on_year, warehouse_schema_decisions.decided_on_month,\n" +
                "  CASE WHEN warehouse_schema_decisions.custom_name IS NOT NULL\n" +
                "        THEN warehouse_schema_decisions.custom_name\n" +
                "        ELSE  warehouse_schema_regimens.name\n" +
                "  END AS regimen_name\n" +
                "FROM warehouse_schema_decisions INNER JOIN warehouse_schema_regimens ON warehouse_schema_regimens.id = warehouse_schema_decisions.regimen_id\n" +
                "GROUP BY \n" +
                "  CASE WHEN warehouse_schema_decisions.custom_name IS NOT NULL\n" +
                "          THEN warehouse_schema_decisions.custom_name\n" +
                "          ELSE  warehouse_schema_regimens.name\n" +
                "  END,\n" +
                "  warehouse_schema_decisions.decided_on_year, warehouse_schema_decisions.decided_on_month\n" +
                "  ORDER BY regimen_name, warehouse_schema_decisions.decided_on_year ASC, warehouse_schema_decisions.decided_on_month ASC");

        int i =0;
        while (rs.next()) {
            i++;
        }
        Assert.assertEquals("Should return 5 rowa", 5, i);
    }

}
