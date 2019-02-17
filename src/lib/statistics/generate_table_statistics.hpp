#pragma once

#include <memory>
#include <unordered_set>

#include "table_statistics.hpp"

namespace opossum {

class Table;
class TableStatistics2;

/**
 * Generate statistics about a Table by analysing its entire data. This may be slow, use with caution.
 */
TableStatistics generate_table_statistics(const Table& table);

void generate_cardinality_estimation_statistics(const std::shared_ptr<Table>& table);
void generate_chunk_pruning_statistics(const std::shared_ptr<Table>& table);

}  // namespace opossum
