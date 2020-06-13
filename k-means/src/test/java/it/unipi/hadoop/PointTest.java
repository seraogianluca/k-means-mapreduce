package it.unipi.hadoop;

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
        System.out.println("Manhattan Test 1 Distance: " + distance);
        assertTrue(Math.abs(distance - 2.0) < 0.0001f);       
    }

    public void testManhattan2() {
        float distance = t1.distance(t2, 1);
        System.out.println("Manhattan Test 2 Distance: " + distance);
        assertTrue(Math.abs(distance - 5.0) < 0.0001f);
    }

    public void testManhattan3() {
        float distance = z1.distance(z2, 1);
        System.out.println("Manhattan Test 3 Distance: " + distance);
        assertTrue(Math.abs(distance - 19.0) < 0.0001f );
    }

    public void testEuclidean1() {
        float distance = p1.distance(p2, 2);
        System.out.println("Euclidean Test 1 Distance: " + distance);
        assertTrue(Math.abs(distance - 1.4142) < 0.0001f );
    }

    public void testEuclidean2() {
        float distance = t1.distance(t2, 2);
        System.out.println("Euclidean Test 2 Distance: " + distance);
        assertTrue(Math.abs(distance - 3.3166) < 0.0001f );
    }

    public void testEuclidean3() {
        float distance = z1.distance(z2, 2);
        System.out.println("Euclidean Test 3 Distance: " + distance);
        assertTrue(Math.abs(distance - 7.9372) < 0.0001f);
    }

    public void testMinkowsky1() {
        float distance = p1.distance(p2, 5);
        System.out.println("Minkowsky Test 1 Distance: " + distance);
        assertTrue(Math.abs(distance - 1.1486) < 0.0001f );
    }

    public void testMinkowsky2() {
        float distance = t1.distance(t2, 5);
        System.out.println("Minkowsky Test 2 Distance: " + distance);
        assertTrue(Math.abs(distance - 3.0049) < 0.0001f );
    }

    public void testMinkowsky3() {
        float distance = z1.distance(z2, 5);
        System.out.println("Minkowsky Test 3 Distance: " + distance);
        assertTrue(Math.abs(distance - 5.2788) < 0.0001f);
    }

    public void testInfinity1() {
        float distance = p1.distance(p2, Integer.MAX_VALUE);
        System.out.println("Infinity Test 1 Distance: " + distance);
        assertTrue(Math.abs(distance - 1.0) < 0.0001f);
    }

    public void testInfinity2() {
        float distance = t1.distance(t2, Integer.MAX_VALUE);
        System.out.println("Infinity Test 2 Distance: " + distance);
        assertTrue(Math.abs(distance - 3.0) < 0.0001f);   
    }

    public void testInfinity3() {
        float distance = z1.distance(z2, Integer.MAX_VALUE);
        System.out.println("Infinity Test 3 Distance: " + distance);
        assertTrue(Math.abs(distance - 5.0) < 0.0001f);
    }

}