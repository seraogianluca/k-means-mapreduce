package it.unipi.hadoop;

import org.junit.Test;
import static org.junit.Assert.*;
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
        p1 = new Point(new float[] {5, -1});
        p2 = new Point(new float[] {4, 2}); 
        //3.16228 euclidea
        t1 = new Point(new float[] {3, 1, -4.2});
        t2 = new Point(new float[] {142.02, 201, -40.2});
        //246.216 euclidea
        z1 = new Point(new float[] {6, 2, 0, 17, 18, 20, 1});
        z2 = new Point(new float[] {-6, 2, -15, 4, -18, 13, 1});
        //43.3935 euclidea
    }
    
    protected void tearDown() throws Exception { 
		super.tearDown();
    }
    
    public void testManhattan1() {
        float distance = p1.distance(p2, 1);
        System.out.println("Mario Distance: " + distance + "Wolfram: ");
        assertTrue(distance - 1 < 0.0001f);       
    }

    public void testManhattan2() {
        float distance = t1.distance(t2, 1);
        System.out.println("Distance: " + distance + "Wolfram:");
        assertTrue(distance - 0 < 0.0001f );
    }

    public void testManhattan3() {
        float distance = z1.distance(z2, 1);
        System.out.println("Distance: " + distance + "Wolfram:");
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