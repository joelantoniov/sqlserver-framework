#! /usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from typing import List, Dict, Any

from core.models import RecommendationConfig, RecommendationHeuristicConfig, DBMSMetricData
from core.metrics_collector import MetricsCollector
# from core.adapters import DatabaseAdapter # Might be needed if direct DB queries are made

logger = logging.getLogger(__name__)

class RecommendationEngine:
    def __init__(self, metrics_collector: MetricsCollector,
                 recommendation_config: RecommendationConfig):
        # self.db_adapter = db_adapter # If needed for live checks
        self.metrics_collector = metrics_collector
        self.config = recommendation_config

    def _evaluate_condition(self, condition_str: str, data: Dict[str, Any]) -> bool:
        """
        Safely evaluates a condition string against data.
        This is a VERY simplified and potentially unsafe evaluator.
        A production system would use a proper expression language (e.g., ast.literal_eval for simple cases,
        or a dedicated library like 'python-json-logic' or a custom parser).
        DO NOT USE eval() with arbitrary strings from config in production.
        """
        try:
            # Example: "avg_user_impact > 80 AND avg_total_user_cost > 1000"
            # This requires 'avg_user_impact' and 'avg_total_user_cost' to be keys in 'data'.
            # For demonstration, we'll manually parse a couple of known conditions.
            if "avg_user_impact" in condition_str and "avg_total_user_cost" in condition_str:
                impact = float(data.get('avg_user_impact', 0))
                cost = float(data.get('avg_total_user_cost', 0))
                # This is a hardcoded interpretation of the example condition string
                return impact > 80 and cost > 1000
            elif "user_seeks == 0" in condition_str and "user_updates > 1000" in condition_str:
                seeks = int(data.get('user_seeks', 0))
                scans = int(data.get('user_scans', 0))
                lookups = int(data.get('user_lookups', 0))
                updates = int(data.get('user_updates', 0))
                return seeks == 0 and scans == 0 and lookups == 0 and updates > 1000

            logger.warning(f"Condition evaluation not implemented for: {condition_str}. Defaulting to False.")
            return False
        except Exception as e:
            logger.error(f"Error evaluating condition '{condition_str}' with data {data}: {e}", exc_info=True)
            return False


    async def generate_recommendations(self) -> List[str]: 
        logger.info("Generating recommendations...")
        recommendations: List[str] = []

        for heuristic_cfg in self.config.heuristics:
            dmv_metric_name = heuristic_cfg.dmv
            metric_data_entries: List[DBMSMetricData] = self.metrics_collector.get_collected_dbms_metrics(
                metric_name_filter=dmv_metric_name
            )

            if not metric_data_entries:
                logger.info(f"No data for DMV '{dmv_metric_name}' for heuristic '{heuristic_cfg.name}'.")
                continue

            for entry in metric_data_entries: 
                row_data = entry.data
                if self._evaluate_condition(heuristic_cfg.condition, row_data):
                    try:
                        rec_text = heuristic_cfg.recommendation_template.format(**row_data)
                        recommendations.append(rec_text)
                        self.metrics_collector.log_recommendation(rec_text)
                    except KeyError as e:
                        logger.error(f"KeyError formatting recommendation for '{heuristic_cfg.name}': {e}. Template: {heuristic_cfg.recommendation_template}, Data: {row_data}", exc_info=True)
                    except Exception as e:
                         logger.error(f"Error formatting recommendation for '{heuristic_cfg.name}': {e}", exc_info=True)

        if not recommendations:
            logger.info("No specific recommendations generated based on current heuristics and data.")

        return recommendations
