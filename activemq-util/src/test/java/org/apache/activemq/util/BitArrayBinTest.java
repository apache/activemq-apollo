package org.apache.activemq.util;

import org.testng.Assert;
import org.testng.annotations.Test;

public class BitArrayBinTest {
        
    @Test()
	public void testSetAroundWindow() throws Exception {
        doTestSetAroundWindow(500, 2000);
        doTestSetAroundWindow(512, 2000);
        doTestSetAroundWindow(128, 512);
    }
       
    private void doTestSetAroundWindow(int window, int dataSize) throws Exception {

        BitArrayBin toTest = new BitArrayBin(window);
        
        for (int i=0; i <= dataSize; i++) {
            Assert.assertTrue((!toTest.setBit(i, Boolean.TRUE)), "not already set");
        }

        int windowOfValidData = roundWindow(dataSize, window);
        int i=dataSize;
        for (; i >= dataSize -windowOfValidData; i--) {
            Assert.assertTrue(toTest.setBit(i, Boolean.TRUE), ("was already set, id=" + i));
        }
        
        for (; i >= 0; i--) {
            Assert.assertTrue((!toTest.setBit(i, Boolean.TRUE)), ("was not already set, id=" + i));
        }
        
        for (int j= dataSize +1; j<(2*dataSize); j++) {
            Assert.assertTrue((!toTest.setBit(j, Boolean.TRUE)), ("not already set: id=" + j));
        }
    }
    
    @Test()
	public void testSetUnsetAroundWindow() throws Exception {
        doTestSetUnSetAroundWindow(500, 2000);
        doTestSetUnSetAroundWindow(512, 2000);
        doTestSetUnSetAroundWindow(128, 512);
    }
    
    private void doTestSetUnSetAroundWindow(int dataSize, int window) throws Exception {

        BitArrayBin toTest = new BitArrayBin(window);
        
        for (int i=0; i <=dataSize; i++) {
            Assert.assertTrue((!toTest.setBit(i, Boolean.TRUE)), "not already set");
        }
                
        int windowOfValidData = roundWindow(dataSize, window);
        for (int i=dataSize; i >= 0 && i >=dataSize -windowOfValidData; i--) {
            Assert.assertTrue(toTest.setBit(i, Boolean.FALSE), ("was already set, id=" + i));
        }

        for (int i=0; i <=dataSize; i++) {
            Assert.assertTrue((!toTest.setBit(i, Boolean.TRUE)), ("not already set, id:" + i));
        }

        for (int j= 2*dataSize; j< 4*dataSize; j++) {
            Assert.assertTrue((!toTest.setBit(j, Boolean.TRUE)), ("not already set: id=" + j));
        }
    }
    
    @Test()
	public void testSetAroundLongSizeMultiplier() throws Exception {
        int window = 512;
        int dataSize = 1000;
        for (int muliplier=1; muliplier <8; muliplier++) {
            for (int value=0; value <dataSize; value++) {
                BitArrayBin toTest = new BitArrayBin(window);
                
                int instance = value +muliplier*BitArray.LONG_SIZE;
                Assert.assertTrue((!toTest.setBit(instance, Boolean.TRUE)), ("not already set: id=" + instance));
                Assert.assertTrue((!toTest.setBit(value, Boolean.TRUE)), ("not already set: id=" + value));
            }
        }
    }
    
    @Test()
	public void testLargeGapInData() throws Exception {
        doTestLargeGapInData(128);
        doTestLargeGapInData(500);
    }
    
    public void doTestLargeGapInData(int window) throws Exception {
        BitArrayBin toTest = new BitArrayBin(window);
        
        int instance = BitArray.LONG_SIZE;
        Assert.assertTrue((!toTest.setBit(instance, Boolean.TRUE)), ("not already set: id=" + instance));
        
        instance = 12 *BitArray.LONG_SIZE;
        Assert.assertTrue((!toTest.setBit(instance, Boolean.TRUE)), ("not already set: id=" + instance));
        
        instance = 9 *BitArray.LONG_SIZE;
        Assert.assertTrue((!toTest.setBit(instance, Boolean.TRUE)), ("not already set: id=" + instance));
    }
    
    // window moves in increments of BitArray.LONG_SIZE.
    // valid data window on low end can be larger than window
    private int roundWindow(int dataSetEnd, int windowSize) {
        
        int validData = dataSetEnd - windowSize;
        int validDataBin = validData / BitArray.LONG_SIZE;
        validDataBin += (windowSize % BitArray.LONG_SIZE > 0? 1:0);
        int startOfValid = validDataBin * BitArray.LONG_SIZE;
        
        return dataSetEnd - startOfValid;        
    }

}
