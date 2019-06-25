const { Client } = require("pg");
const _ = require("lodash");

module.exports = async function InformationSchema(options) {
  options = {table_schema: 'public', ...options}
  const client = new Client(options);
  await client.connect();

  async function query(statement) {
    return (await client.query(statement)).rows;
  }

  const tables = await query(`
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_schema = '${options.table_schema}';
    `);

  const columns = await query(`
        SELECT table_name, column_name, column_default, is_nullable, data_type
        FROM information_schema.columns
        WHERE table_schema = '${options.table_schema}';
    `);

  const constrains = await query(`
        SELECT kcu.table_name, kcu.column_name, kcu.constraint_name, tc.constraint_type
        FROM information_schema.key_column_usage AS kcu
        LEFT JOIN information_schema.table_constraints AS tc
        ON tc.constraint_name = kcu.constraint_name
        WHERE tc.table_schema = '${options.table_schema}'
        AND tc.constraint_type IN ('FOREIGN KEY', 'PRIMARY KEY');
    `);

  const constraintColumnUsage = await query(`
        SELECT table_name, column_name, constraint_name
        FROM information_schema.constraint_column_usage
        WHERE table_schema = '${options.table_schema}';
    `);

  await client.end()

  const tablesMap = _.reduce(
    tables,
    (acc, pres) => {
      acc[pres.table_name] = { columns: {}, relations: {} };
      return acc;
    },
    {}
  );

  const columnsMap = _.reduce(
    columns,
    (acc, pres) => {
      acc[pres.table_name].columns[pres.column_name] = _.omit(pres, [
        "table_name",
        "column_name"
      ]);
      return acc;
    },
    tablesMap
  );

  const ccuMap = _.reduce(
    constraintColumnUsage,
    (acc, pre) => {
      acc[pre.constraint_name] = pre;
      return acc;
    },
    {}
  );

  return _.reduce(
    constrains,
    (acc, pres) => {
      if (pres.constraint_type === "PRIMARY KEY") {
        acc[pres.table_name].columns[pres.column_name].primary = true;
        acc[pres.table_name].primary_key = pres.column_name;
      } else if (pres.constraint_type === "FOREIGN KEY") {
        const refernceTable = ccuMap[pres.constraint_name];
        const currentTable = pres;

        acc[currentTable.table_name].columns[
          currentTable.column_name
        ].reference = _.pick(refernceTable, ["table_name", "column_name"]);

        if (!acc[refernceTable.table_name].relations[currentTable.table_name]) {
          acc[refernceTable.table_name].relations[currentTable.table_name] = {};
        }

        acc[refernceTable.table_name].relations[currentTable.table_name][
          currentTable.column_name
        ] = { type: "one-many" };
      }
      return acc;
    },
    columnsMap
  );
};
