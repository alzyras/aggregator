from __future__ import annotations

from django.test import SimpleTestCase

from providers.asana.planner_badges import planner_badges as asana_badges
from providers.habitica.planner_badges import planner_badges as habitica_badges
from providers.github_issues.planner_badges import planner_badges as github_badges
from providers.jira.planner_badges import planner_badges as jira_badges
from providers.linear.planner_badges import planner_badges as linear_badges
from providers.todoist.planner_badges import planner_badges as todoist_badges


class ProviderBadgeTests(SimpleTestCase):
    def test_jira_badges_include_project_and_epic(self):
        raw = {"__jira_planner_context": {"project": "DEVT", "epic": "Competition mode"}}

        badges = jira_badges(None, raw)

        self.assertEqual(badges, ["DEVT", "Competition mode"])

    def test_jira_badges_omit_missing_epic(self):
        raw = {"__jira_planner_context": {"project": "DEVT"}}

        badges = jira_badges(None, raw)

        self.assertEqual(badges, ["DEVT"])

    def test_asana_badges_include_workspace_and_project(self):
        raw = {"__asana_planner_context": {"workspace_name": "Product", "project_name": "Launch"}}

        badges = asana_badges(None, raw)

        self.assertEqual(badges, ["Product", "Launch"])

    def test_todoist_badges_include_project_and_section(self):
        raw = {"__todoist_planner_context": {"project_name": "Ops", "section_name": "Today"}}

        badges = todoist_badges(None, raw)

        self.assertEqual(badges, ["Ops", "Today"])

    def test_habitica_badges_include_type_and_one_tag(self):
        raw = {"__habitica_planner_context": {"task_type": "Todo", "tags": ["Work", "Urgent"]}}

        badges = habitica_badges(None, raw)

        self.assertEqual(badges, ["Todo", "Work"])

    def test_github_badges_include_repository_and_labels(self):
        raw = {
            "__github_planner_context": {
                "repository": "acme/app",
                "labels": ["bug", "priority:high", "backend"],
            }
        }

        self.assertEqual(github_badges(None, raw), ["acme/app", "bug", "priority:high"])

    def test_linear_badges_prioritize_identifier_team_and_project(self):
        raw = {
            "__linear_planner_context": {
                "identifier": "ENG-42",
                "team": "ENG",
                "project": "Unified inbox",
                "priority": "High",
            }
        }

        self.assertEqual(linear_badges(None, raw), ["ENG-42", "ENG", "Unified inbox"])
