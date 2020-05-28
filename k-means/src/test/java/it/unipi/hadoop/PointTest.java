package it.unipi.hadoop;

import org.apache.hadoop.io.FloatWritable;

import it.unipi.hadoop.model.Point;
import junit.framework.TestCase;

public class PointTest extends TestCase {

    private Point p1; // bi-dimensional point
    private Point p2; // bi-dimensional point

    private Point t1; // tri-dimensional point
    private Point t2; // tri-dimensional point

    private Point z1; // 7-dimensional point
    private Point z2; // 7-dimensional point

    protected void setUp() throws Exception {
        super.setUp();
        //XA = [(1, 2, 3, -4, 5, 6, 7)]
        //XB = [(2, 1, 0, 1, 2, 3, 4)]
        p1 = new Point(new float[] {1, 2});
        p2 = new Point(new float[] {2, 1}); 
     
        t1 = new Point(new float[] {1, 2, 3});
        t2 = new Point(new float[] {2, 1, 0});
     
        z1 = new Point(new float[] {1, 2, 3, -4, 5, 6, 7});
        z2 = new Point(new float[] {2, 1, 0, 1, 2, 3, 4});
   
    }
    
    protected void tearDown() throws Exception { 
		super.tearDown();
    }
    
    public void testManhattan1() {
        float distance = p1.distance(p2, 1);
        System.out.println("Mario Distance: " + distance);
        assertTrue(true);       
    }

    public void testManhattan2() {
        float distance = t1.distance(t2, 1);
        System.out.println("Distance: " + distance);
        assertTrue(distance - 0 < 0.0001f );
    }

    public void testManhattan3() {
        float distance = z1.distance(z2, 1);
        System.out.println("Distance: " + distance);
        assertTrue(distance - 0  < 0.0001f );
    }

    public void testEuclidean1() {

    }

    public void testEuclidean2() {
        
    }

    public void testMinkowsky1() {

    }

    public void testMinkowsky2() {

    }

    public void testMinkowsky3() {

    }

    public void testInfinity1() {

    }

    public void testInfinity() {

    }

 }