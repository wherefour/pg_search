# frozen_string_literal: true

require "digest"

module PgSearch
  class Configuration
    class Association
      attr_reader :columns, :options, :model

      def initialize(model, name, options)
        @model = model
        @name = name
        @options = options
        @columns = Array(options[:against]).map do |column_name, weight|
          ForeignColumn.new(column_name, weight, @model, self)
        end
      end

      def table_name
        @model.reflect_on_association(@name).table_name
      end

      def join(primary_key)
        "LEFT OUTER JOIN (#{relation(primary_key).to_sql}) #{subselect_alias} ON #{subselect_alias}.id = #{primary_key}"
      end

      def subselect_alias
        Configuration.alias(table_name, @name, "subselect")
      end

      def tsvector_for_column(column_name)
        options.dig(:using, :tsearch, :tsvector_column).find { |column| column == "#{column_name}_tsvector" }
      end

      private

      def selects
        if singular_association?
          selects_for_singular_association
        else
          selects_for_multiple_association
        end
      end

      def selects_for_singular_association
        columns.map do |column|
          tsvector_column = tsvector_for_column(column.name)
          if tsvector_column
            "#{model.connection.quote_table_name(table_name)}.#{tsvector_column}::tsvector AS #{Configuration.alias(subselect_alias, tsvector_column)}"
          else
            "#{column.full_name}::text AS #{column.alias}"
          end
        end.join(", ")
      end

      def selects_for_multiple_association
        columns.map do |column|
          tsvector_column = tsvector_for_column(column.name)
          if tsvector_column
            "string_agg(#{model.connection.quote_table_name(table_name)}.#{tsvector_column}::tsvector, ' ') AS #{Configuration.alias(subselect_alias, tsvector_column)}"
          else
            "string_agg(#{column.full_name}::text, ' ') AS #{column.alias}"
          end
        end.join(", ")
      end

      def relation(primary_key)
        result = @model.unscoped.joins(@name).select("#{primary_key} AS id, #{selects}")
        result = result.group(primary_key) unless singular_association?
        result
      end

      def singular_association?
        %i[has_one belongs_to].include?(@model.reflect_on_association(@name).macro)
      end
    end
  end
end
