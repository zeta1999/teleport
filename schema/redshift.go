package schema

var generateRedshiftDataTypeExpression = func(column Column) (string, bool) {
	switch column.DataType {
	case TEXT:
		return "VARCHAR(65535)", true
	}

	return "", false
}

var redshiftDriverExtensions = DriverExtensions{
	GenerateDataTypeExpression: &generateRedshiftDataTypeExpression,
}
