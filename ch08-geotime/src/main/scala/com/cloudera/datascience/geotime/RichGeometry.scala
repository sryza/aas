/*
 * Copyright 2015 Sanford Ryza, Uri Laserson, Sean Owen and Joshua Wills
 *
 * See LICENSE file for further information.
 */
package com.cloudera.datascience.geotime

import com.esri.core.geometry.{GeometryEngine, SpatialReference, Geometry}

import scala.language.implicitConversions

/**
 * A wrapper that provides convenience methods for using the spatial relations in the ESRI
 * GeometryEngine with a particular instance of the Geometry interface and an associated
 * SpatialReference.
 *
 * @param geometry the geometry object
 * @param csr optional spatial reference; if not specified, uses WKID 4326 a.k.a. WGS84, the
 *                         standard coordinate frame for Earth.
 */
class RichGeometry(val geometry: Geometry,
    val csr: SpatialReference = SpatialReference.create(4326)) extends Serializable {

  def area2D(): Double = geometry.calculateArea2D()

  def distance(other: Geometry): Double = {
    GeometryEngine.distance(geometry, other, csr)
  }

  def contains(other: Geometry): Boolean = {
    GeometryEngine.contains(geometry, other, csr)
  }

  def within(other: Geometry): Boolean = {
    GeometryEngine.within(geometry, other, csr)
  }

  def overlaps(other: Geometry): Boolean = {
    GeometryEngine.overlaps(geometry, other, csr)
  }

  def touches(other: Geometry): Boolean = {
    GeometryEngine.touches(geometry, other, csr)
  }

  def crosses(other: Geometry): Boolean = {
    GeometryEngine.crosses(geometry, other, csr)
  }

  def disjoint(other: Geometry): Boolean = {
    GeometryEngine.disjoint(geometry, other, csr)
  }
}

/**
 * Helper object for implicitly creating RichGeometry wrappers
 * for a given Geometry instance.
 */
object RichGeometry extends Serializable {
  implicit def createRichGeometry(g: Geometry): RichGeometry = new RichGeometry(g)
}