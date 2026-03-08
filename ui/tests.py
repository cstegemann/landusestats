from pathlib import Path

from django.conf import settings
from django.test import TestCase
from django.urls import reverse

from geo.models import AdminBoundary, BaseDataset, DerivedDataset


class InspectDatasetViewTests(TestCase):
    def setUp(self):
        self.base = BaseDataset.objects.create(
            name="berlin",
            relative_path="base/berlin.gpkg",
            current_version=1,
        )
        base_file = Path(settings.GEO_DATA_DIR) / "base" / "berlin.gpkg"
        base_file.parent.mkdir(parents=True, exist_ok=True)
        base_file.touch()

        self.derived = DerivedDataset.objects.create(
            base=self.base,
            name="berlin_admin_v1",
            kind=DerivedDataset.Kind.ADMINBOUNDARIES,
            version=1,
            relative_path="derived/berlin_v1_adminboundaries.gpkg",
        )
        self.top_boundary = AdminBoundary.objects.create(
            dataset=self.derived,
            version=1,
            extraction_algo_version=1,
            source="OSM",
            external_id="top-1",
            admin_level=4,
            name="Top Boundary",
            geom_geojson={
                "type": "Polygon",
                "coordinates": [[[13.0, 52.0], [13.2, 52.0], [13.2, 52.2], [13.0, 52.2], [13.0, 52.0]]],
            },
        )
        derived_file = Path(settings.GEO_DATA_DIR) / "derived" / f"{self.base.name}_v{self.base.current_version}_adminboundaries.gpkg"
        derived_file.parent.mkdir(parents=True, exist_ok=True)
        derived_file.touch()

        self.target_boundary = AdminBoundary.objects.create(
            dataset=self.derived,
            version=1,
            extraction_algo_version=1,
            source="OSM",
            external_id="berlin-8",
            admin_level=8,
            name="Berlin Mitte",
            geom_geojson={
                "type": "Polygon",
                "coordinates": [[[13.35, 52.5], [13.45, 52.5], [13.45, 52.55], [13.35, 52.55], [13.35, 52.5]]],
            },
        )

    def test_overview_shows_inspect_link_for_in_sync_dataset(self):
        response = self.client.get(reverse("base_data_overview"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, reverse("inspect_dataset", kwargs={"base_id": self.base.id}))

    def test_inspect_dataset_renders_overlay_context(self):
        response = self.client.get(reverse("inspect_dataset", kwargs={"base_id": self.base.id}))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Inspect")
        self.assertContains(response, "Overlay level")
        self.assertContains(response, "Top Boundary")

    def test_inspect_search_filters_by_name_and_level(self):
        response = self.client.get(
            reverse("inspect_search", kwargs={"base_id": self.base.id}),
            {"q": "Mitte"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Berlin Mitte")
        self.assertNotContains(response, "Top Boundary")

    def test_inspect_boundary_returns_partial(self):
        response = self.client.get(
            reverse(
                "inspect_boundary",
                kwargs={"base_id": self.base.id, "boundary_id": self.target_boundary.id},
            )
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Generate landuse stats")
        self.assertContains(response, "Berlin Mitte")
