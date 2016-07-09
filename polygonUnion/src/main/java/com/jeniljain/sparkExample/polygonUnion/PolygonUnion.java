package com.jeniljain.sparkExample.polygonUnion;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.operation.union.CascadedPolygonUnion;

public class PolygonUnion {
	public static void main(String[] args) {
		
		System.out.println("Main method started");
		SparkConf sparkConfig = new SparkConf().setAppName("ClosestPair");
		JavaSparkContext context = new JavaSparkContext(sparkConfig);
		
		JavaRDD<String> lines = context.textFile(args[0]);
		JavaRDD<Geometry> MappedPolygons = lines.mapPartitions(new LocalPolygonUnion());
		JavaRDD<Geometry> ReduceList = MappedPolygons.coalesce(1);
		JavaRDD<Geometry> FinalList = ReduceList.mapPartitions(new GlobalPolygonUnion());
		FinalList.saveAsTextFile(args[1]);
	}
}

class LocalPolygonUnion implements FlatMapFunction<Iterator<String>, Geometry>, Serializable{

	private static final long serialVersionUID = 1L;

	public Iterable<Geometry> call(Iterator<String> lines) throws Exception {
		
		List<Geometry> Polygons = new ArrayList<Geometry>();
		
		while(lines.hasNext()){
			String line = lines.next();
			String[] CoordList = line.split(",");
			Double x1 = Double.parseDouble(CoordList[0]);
			Double y1 = Double.parseDouble(CoordList[1]);
			Double x2 = Double.parseDouble(CoordList[2]);
			Double y2 = Double.parseDouble(CoordList[3]);
			
			Coordinate[] coordinates = new Coordinate[5];
			coordinates[0] = new Coordinate(x1,y1);
			coordinates[1] = new Coordinate(x1,y2);
			coordinates[2] = new Coordinate(x2,y2);
			coordinates[3] = new Coordinate(x2,y1);
			coordinates[4] = new Coordinate(x1,y1);
			
			GeometryFactory geomFact = new GeometryFactory();
			LinearRing linear = new GeometryFactory().createLinearRing(coordinates);
			Polygon poly = new Polygon(linear, null, geomFact);
			Polygons.add(poly);
		}
		
		Collection<Geometry> unionPolygon = Polygons;
		CascadedPolygonUnion cascadedPoly = new CascadedPolygonUnion(unionPolygon);
		List<Geometry> localPolygonList = new ArrayList<Geometry>();
		localPolygonList.add((Geometry) cascadedPoly.union());
		return localPolygonList;
	}
}

class GlobalPolygonUnion implements FlatMapFunction<Iterator<Geometry>, Geometry>, Serializable{

	private static final long serialVersionUID = 1L;

	public Iterable<Geometry> call(Iterator<Geometry> geoms) throws Exception {
		List<Geometry> Polygons = new ArrayList<Geometry>();
		
		while(geoms.hasNext()){ Polygons.add(geoms.next()); }
		
		Collection<Geometry> unionPolygon = Polygons;
		CascadedPolygonUnion cascadedPoly = new CascadedPolygonUnion(unionPolygon);
		List<Geometry> FinalList = new ArrayList<Geometry>();
		FinalList.add((Geometry) cascadedPoly.union());
		return FinalList;
	}
}
