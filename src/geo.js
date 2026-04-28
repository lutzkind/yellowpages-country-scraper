const turf = require("@turf/turf");

function parseBoundingBox(rawBoundingBox) {
  if (!Array.isArray(rawBoundingBox) || rawBoundingBox.length !== 4) {
    throw new Error("Invalid country bounding box.");
  }
  const [south, north, west, east] = rawBoundingBox.map((v) => Number.parseFloat(v));
  return { south, west, north, east };
}

function bboxToArray(bbox) {
  return [bbox.west, bbox.south, bbox.east, bbox.north];
}

function bboxFromArray(raw) {
  if (!Array.isArray(raw) || raw.length !== 4) {
    throw new Error("Invalid bbox array.");
  }
  const [west, south, east, north] = raw.map((v) => Number.parseFloat(v));
  return { south, west, north, east };
}

function splitBBox(bbox) {
  const midLat = (bbox.south + bbox.north) / 2;
  const midLon = (bbox.west + bbox.east) / 2;
  return [
    { south: bbox.south, west: bbox.west, north: midLat, east: midLon },
    { south: bbox.south, west: midLon, north: midLat, east: bbox.east },
    { south: midLat, west: bbox.west, north: bbox.north, east: midLon },
    { south: midLat, west: midLon, north: bbox.north, east: bbox.east },
  ];
}

function canSplitBBox(bbox, config) {
  return (
    bbox.east - bbox.west > config.minShardWidthDeg &&
    bbox.north - bbox.south > config.minShardHeightDeg
  );
}

function bboxIntersectsGeometry(bbox, geometry) {
  if (!geometry) return true;
  return turf.booleanIntersects(turf.bboxPolygon(bboxToArray(bbox)), geometry);
}

function bboxCenter(bbox) {
  return { lat: (bbox.south + bbox.north) / 2, lon: (bbox.west + bbox.east) / 2 };
}

function deriveSeedBBoxes(geometry, fallbackBbox, options = {}) {
  if (!geometry) {
    return [fallbackBbox];
  }

  const {
    maxSeeds = 32,
    cumulativeAreaRatio = 0.995,
    minPolygonAreaRatio = 0.0002,
  } = options;

  const feature = geometry.type === "Feature" ? geometry : { type: "Feature", geometry };
  const polygons = [];

  turf.flattenEach(feature, (polygonFeature) => {
    const area = turf.area(polygonFeature);
    if (!Number.isFinite(area) || area <= 0) return;
    polygons.push({
      area,
      bbox: bboxFromArray(turf.bbox(polygonFeature)),
    });
  });

  if (polygons.length === 0) {
    return [fallbackBbox];
  }

  polygons.sort((a, b) => b.area - a.area);

  const totalArea = polygons.reduce((sum, polygon) => sum + polygon.area, 0);
  const filteredPolygons = polygons.filter((polygon) => {
    if (!Number.isFinite(totalArea) || totalArea <= 0) return true;
    return (polygon.area / totalArea) >= minPolygonAreaRatio;
  });
  const targetArea = totalArea * cumulativeAreaRatio;
  const seedBBoxes = [];
  let coveredArea = 0;

  for (const polygon of filteredPolygons) {
    seedBBoxes.push(polygon.bbox);
    coveredArea += polygon.area;

    if (seedBBoxes.length >= maxSeeds) break;
    if (coveredArea >= targetArea) break;
  }

  if (seedBBoxes.length > 0) {
    return seedBBoxes;
  }

  return polygons.length > 0 ? [polygons[0].bbox] : [fallbackBbox];
}

function bboxRadiusMeters(bbox, capMeters = Number.POSITIVE_INFINITY) {
  const center = bboxCenter(bbox);
  const corners = [
    [bbox.west, bbox.south], [bbox.east, bbox.south],
    [bbox.west, bbox.north], [bbox.east, bbox.north],
  ];
  const maxKm = Math.max(
    ...corners.map(([lon, lat]) =>
      turf.distance(turf.point([center.lon, center.lat]), turf.point([lon, lat]))
    )
  );
  return Math.min(Math.ceil(maxKm * 1000), capMeters);
}

function pointInsideBBox(lat, lon, bbox) {
  return lat >= bbox.south && lat <= bbox.north && lon >= bbox.west && lon <= bbox.east;
}

function pointInsideGeometry(lat, lon, geometry) {
  if (!geometry) return true;
  return turf.booleanPointInPolygon(turf.point([lon, lat]), geometry);
}

module.exports = {
  parseBoundingBox, bboxToArray, splitBBox, canSplitBBox,
  bboxIntersectsGeometry, bboxCenter, bboxRadiusMeters,
  pointInsideBBox, pointInsideGeometry, deriveSeedBBoxes,
};
