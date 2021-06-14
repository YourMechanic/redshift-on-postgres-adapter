require 'active_record/tasks/postgresql_database_tasks'

class ActiveRecord::Tasks::RedshiftDatabaseTasks < ActiveRecord::Tasks::PostgreSQLDatabaseTasks # :nodoc:

  def initialize(db_config)
    super(db_config)
  end

  def structure_dump(filename)
    set_psql_env

    search_path = \
    case ActiveRecord::Base.dump_schemas
    when :schema_search_path
      configuration[:schema_search_path]
    when :all
      nil
    when String
      ActiveRecord::Base.dump_schemas
    end

    File.open(filename, 'w+') do |file|
      tbl_ddl_results.each_row do |row|
        file.puts(row)
      end

      view_ddl_results.each_row do |row|
        ddl = row[0].split("\n")
        # Remove --DROP statement
        lines_to_remove = 1

        if row[0].include?('CREATE MATERIALIZED VIEW')
          # Hack to remove additional create or replace view added in case its MATERIALIZED VIEW
          lines_to_remove = 2
        end
        ddl = ddl[lines_to_remove..-1]
        file.puts(ddl + ["\n"])
      end
    end
    File.open(filename, "a") { |f| f << "SET search_path TO #{connection.schema_search_path};\n\n" }
  end

  def structure_load(filename)
    sql = nil
    if File.exist?(output_location)
      sql = File.read(output_location)
    else
      puts 'Schema Dump file does not exist. Run task db:structure:dump'
      false
    end
    connection.execute(sql) if sql.present?
  end

  def tbl_ddl_results(ddl_tbl = 'admin.v_generate_tbl_ddl')
    ddl_sql = <<-SQL
        SELECT  ddl
        FROM    #{ddl_tbl}
        WHERE   schemaname = '#{connection.schema_search_path}'
        AND tablename not ilike 'mv_tbl_%' AND  (ddl NOT ilike '%owner to%' AND ddl NOT ilike '--DROP TABLE%')
        ORDER BY tablename ASC, seq ASC
        SQL
    connection.execute(ddl_sql)
  end

  def view_ddl_results(ddl_tbl = 'admin.v_generate_view_ddl')
    ddl_sql = <<-SQL
        SELECT  ddl
        FROM    #{ddl_tbl}
        WHERE   schemaname = '#{connection.schema_search_path}'
        ORDER BY viewname ASC
        SQL
    connection.execute(ddl_sql)
  end
end