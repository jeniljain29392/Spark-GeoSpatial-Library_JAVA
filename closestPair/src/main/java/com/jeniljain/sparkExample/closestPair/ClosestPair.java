package com.jeniljain.sparkExample.closestPair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import com.vividsolutions.jts.geom.Coordinate;

public class ClosestPair {
	public static void main(String[] args) {
		
		System.out.println("Main method started");
		SparkConf sparkConfig = new SparkConf().setAppName("ClosestPair");
		JavaSparkContext context = new JavaSparkContext(sparkConfig);
		
		JavaRDD<String> allPoints = context.textFile(args[0]);
		JavaRDD<Coordinate> closestPair = allPoints.mapPartitions(new FlatMapFunction<Iterator<String>, Coordinate>(){
			private static final long serialVersionUID = 1L;
			
			public Iterable<Coordinate> call(Iterator<String> coords) throws Exception {
				// TODO Auto-generated method stub
				List<Coordinate> ListOfCoordinates = new ArrayList<Coordinate>();
				while (coords.hasNext()) {
					String[] fields = coords.next().split(",");
					ListOfCoordinates.add(new Coordinate(Double.parseDouble(fields[0]),Double.parseDouble(fields[1])));
				}
				Pair closestPair = ClosestPairHelperFunctions.divideAndConquer(ListOfCoordinates);
				List<Coordinate> closestCoordinates = new ArrayList<Coordinate>();
				closestCoordinates.add(closestPair.getPoint1());
				closestCoordinates.add(closestPair.getPoint2());

				return closestCoordinates;
			}
		});
		
		
		JavaRDD<Coordinate> temp = closestPair.repartition(1);
		JavaRDD<String> finalClosestPair = temp.mapPartitions(new FlatMapFunction<Iterator<Coordinate>, String>(){
			private static final long serialVersionUID = 1L;
			
			public Iterable<String> call(Iterator<Coordinate> coords) throws Exception {
				// TODO Auto-generated method stub
				List<Coordinate> ListOfCoordinates = new ArrayList<Coordinate>();
				while (coords.hasNext()) {
					ListOfCoordinates.add(coords.next());
				}
				Pair closestPair = ClosestPairHelperFunctions.divideAndConquer(ListOfCoordinates);
				List<String> closestCoordinates = new ArrayList<String>();
				closestCoordinates.add(closestPair.getPoint1().x + ", " + closestPair.getPoint1().y);
				closestCoordinates.add(closestPair.getPoint2().x + ", " + closestPair.getPoint2().y);

				return closestCoordinates;
			}
		});
		finalClosestPair.sortBy( new Function<String,String>() {
			public String call(String str) throws Exception {return str;}
		}, true, 1 ).saveAsTextFile(args[1]);
		context.close();
	}
}

class ClosestPairHelperFunctions {
	public static Pair divideAndConquer(List<Coordinate> listOfCoordinates) {
		List<Coordinate> coordinatesSortedByX = new ArrayList<Coordinate>(listOfCoordinates);
		sortByXCoordinate(coordinatesSortedByX);
		List<Coordinate> coordinatesSortedByY = new ArrayList<Coordinate>(listOfCoordinates);
		sortByYCoordinate(coordinatesSortedByY);
		return divideAndConquer(coordinatesSortedByX, coordinatesSortedByY);
	}

	private static void sortByYCoordinate(List<Coordinate> coordinatesSortedByY) {
		Collections.sort(coordinatesSortedByY, new Comparator<Coordinate>() {
			public int compare(Coordinate co1, Coordinate co2) {
				if (co1.y < co2.y) return -1;
				if (co1.y > co2.y) return 1;
				return 0;
			}
		});
	}

	private static void sortByXCoordinate(List<Coordinate> coordinatesSortedByX) {
		Collections.sort(coordinatesSortedByX, new Comparator<Coordinate>() {
			public int compare(Coordinate co1, Coordinate co2) {
				if (co1.x < co2.x) return -1;
				if (co1.x > co2.x) return 1;
				return 0;
			}
		});
	}

	private static Pair divideAndConquer(List<Coordinate> coordinatesSortedByX, List<Coordinate> coordinatesSortedByY) {
		int coordinateSize = coordinatesSortedByX.size();
	    if (coordinateSize <= 3)
	      return bruteForceApproach(coordinatesSortedByX);
		
	    int midpoint = coordinateSize >>> 1;
		List<Coordinate> leftPart = coordinatesSortedByX.subList(0, midpoint);
		List<Coordinate> rightPart = coordinatesSortedByX.subList(midpoint, coordinateSize);
		
		List<Coordinate> yLeftPart = new ArrayList<Coordinate>(leftPart);
		sortByYCoordinate(yLeftPart);
		Pair ClosestPairLeft = divideAndConquer(leftPart, yLeftPart);

		List<Coordinate> yRightPart = new ArrayList<Coordinate>(rightPart);
		sortByYCoordinate(yRightPart);
		Pair ClosestPairRight = divideAndConquer(rightPart, yRightPart);

		Pair closestPair = new Pair();
		if (ClosestPairLeft.getDistance() < ClosestPairRight.getDistance()) {
			closestPair = ClosestPairLeft;
		} else closestPair = ClosestPairRight;

		double minDistance = closestPair.getDistance();
		double centerX = rightPart.get(0).x;
		List<Coordinate> tempList = new ArrayList<Coordinate>();
		for (Coordinate coordinate : coordinatesSortedByY) {
			if (Math.abs(centerX - coordinate.x) < minDistance) {
				tempList.add(coordinate);
			}
		}

		for (int i = 0; i < tempList.size() - 1; i++) {
			for (int j = i + 1; j < tempList.size(); j++) {
				if ((tempList.get(j).y - tempList.get(i).y) >= minDistance)
					break;
				double dist = tempList.get(i).distance(tempList.get(j));
				if (dist < closestPair.getDistance()) {
					closestPair.setPoint1(tempList.get(i));
					closestPair.setPoint2(tempList.get(j));
					closestPair.setDistance(dist);
					minDistance = dist;
				}
			}
		}

		return closestPair;
	}

	private static Pair bruteForceApproach(List<Coordinate> coordinatesSortedByX) {
		int coordinateSize = coordinatesSortedByX.size();
		if (coordinateSize < 2) return null;
		Pair closestPair = new Pair(coordinatesSortedByX.get(0), coordinatesSortedByX.get(1));
		if (coordinateSize > 2) {
			for (int i = 0; i < coordinateSize - 1; i++) {
				for (int j = i + 1; j < coordinateSize; j++) {
					double dist = coordinatesSortedByX.get(i).distance(coordinatesSortedByX.get(j));
					if (dist < closestPair.getDistance()) {
						closestPair.setPoint1(coordinatesSortedByX.get(i));
						closestPair.setPoint2(coordinatesSortedByX.get(j));
						closestPair.setDistance(dist);
					}
				}
			}
		}
		return closestPair;
	}
}


class Pair implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private Coordinate point1 = null; 
	private Coordinate point2 = null;
	private double distance = 0.0;
	
	public Pair(Coordinate point1, Coordinate point2) {
		this.setPoint1(point1);
		this.setPoint2(point2);
		this.setDistance(distance(point1, point2));
	}

	public Pair() {
		// TODO Auto-generated constructor stub
	}

	private double distance(Coordinate point1, Coordinate point2) {
		return point1.distance(point2);
	}

	public Coordinate getPoint1() {
		return point1;
	}

	public void setPoint1(Coordinate point1) {
		this.point1 = point1;
	}

	public Coordinate getPoint2() {
		return point2;
	}

	public void setPoint2(Coordinate point2) {
		this.point2 = point2;
	}

	public double getDistance() {
		return distance;
	}

	public void setDistance(double distance) {
		this.distance = distance;
	}
}