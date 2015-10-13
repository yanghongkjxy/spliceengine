package com.splicemachine.db.iapi.types;

import com.splicemachine.db.iapi.error.StandardException;
import org.junit.Test;

import java.math.BigDecimal;

import static junit.framework.TestCase.fail;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;

/**
 * Tests for SQLDecimal class computations
 */

public class SQLDecimalTest {

    /*
    * Rounding mode. It is applied in three cases:
    * 1. Creation
    * 2. Division
    * 3. Manual scale setting
    */

    @Test
    public void testRoundingCreation() {
        BigDecimal positive = new BigDecimal(1000.25);
        BigDecimal negative = new BigDecimal(-1000.5);
        try {
            SQLDecimal positiveDecimal = new SQLDecimal(positive, 4, 1);
            SQLDecimal negativeDecimal = new SQLDecimal(negative, 4, 0);
            assertThat("Rounding must be half-up", positiveDecimal.getDouble(), equalTo(1000.3d));
            assertThat("Rounding must be half-up", negativeDecimal.getDouble(), equalTo(-1001d));
        } catch (StandardException e) {
            fail("BigDecimal must be created properly");
        }
    }

    @Test
    public void testRoundingDividing() {
        SQLDecimal one = new SQLDecimal(BigDecimal.valueOf(155, 1));
        SQLDecimal two = new SQLDecimal(BigDecimal.valueOf(10, 0));
        try {
            NumberDataValue three = one.divide(one, two, null, 0);
            assertThat("Rounding must be half-up", three.getDouble(), equalTo(2.0));
        } catch (Exception e) {
            fail("Division must be performed properly");
        }
    }

    @Test
    public void testRoundingSettingWidth() {
        SQLDecimal one = new SQLDecimal(BigDecimal.valueOf(105, 1));
        SQLDecimal two = new SQLDecimal(BigDecimal.valueOf(49, 2));
        try {
            one.setWidth(2, 0, false);
            two.setWidth(1, 0, false);
            assertThat("Rounding mus be half up", one.getDouble(), equalTo(11d));
            assertThat("Rounding mus be half up", two.getDouble(), equalTo(0d));
        } catch (StandardException e) {
            fail("Rounding must be performed properly");
        }
    }

    @Test
    public void testNegativeRounding() {
        SQLDecimal one = new SQLDecimal(BigDecimal.valueOf(-49, 2));
        SQLDecimal two = new SQLDecimal(BigDecimal.valueOf(-5, 1));
        try {
            one.setWidth(1, 0, false);
            two.setWidth(1, 0, false);
            assertThat("Rounding mus be half up", one.getDouble(), equalTo(0d));
            assertThat("Rounding mus be half up", two.getDouble(), equalTo(-1d));
        } catch (StandardException e) {
            fail("Rounding must be performed properly");
        }
    }


}
