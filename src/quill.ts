import pkg, { Column } from "node-sql-parser";
const { Parser } = pkg;
import { Select, AST, Binary, Interval, ColumnRefItem, Function, AggrFunc, Cast, Param, Value, ExpressionValue, ExprList, ValueExpr, LocationRange, Expr } from "node-sql-parser";

// TODO: own definition
export type extract_func = { type: 'extract'; args: { field: string; source: any; } };

interface DateFilter {
    type: 'current' | 'last' | 'next' | 'previous';
    numberOfPeriods: number;
    period: 'days' | 'weeks' | 'months' | 'quarters' | 'years'; // TODO: Added years; supposed to delete it
    field: string;
};

// all means current_date
interface DateValue {
    type: "date_value";
    numberOfPeriods: number;
    period: 'days' | 'weeks' | 'months' | 'quarters' | 'years' | 'all' | 'current'; // 'all' refers to WHERE transaction_date <= ... (transaction_date)
};

function getTruncatedDate(dateValue: DateValue, newPeriod: 'days' | 'weeks' | 'months' | 'quarters' | 'years'): DateValue {
    const periods = new Map<string, number>([['days', 1], ['weeks', 7], ['months', 30], ['quarters', 90], ['years', 365]]);
    const totalDays = dateValue.numberOfPeriods * periods.get(dateValue.period);
    return {
        type: "date_value",
        numberOfPeriods: Math.floor(totalDays / periods.get(newPeriod)),
        period: newPeriod,
    } as DateValue;
};

const isDateValue = (value: DateValue): value is DateValue => !!(typeof (value?.numberOfPeriods) !== 'undefined') && ['days', 'weeks', 'months', 'quarters', 'all'].includes(value?.period);

function parsePeriod(unit: string): DateFilter['period'] | null {
    switch (unit.toLowerCase()) {
        case 'day':
        case 'days':
            return 'days';
        case 'week':
        case 'weeks':
            return 'weeks';
        case 'month':
        case 'months':
            return 'months';
        case 'quarter':
        case 'quarters':
            return 'quarters';
        case 'year':
        case 'years':
            return 'years'
        default:
            return null; // Invalid unit
    };
};

type ASTNode = ColumnRefItem | Param | Function | Cast | AggrFunc | Value | Binary | Interval | ExpressionValue | ExprList | extract_func | DateValue;

interface NodeVisitor<T> {
    visitColumnRefNode: (columnRefNode: ColumnRefItem) => T;
    visitParamNode: (paramNode: Param) => T;
    visitFunctionNode: (functionNode: Function) => T;
    visitCastNode: (castNode: Cast) => T;
    visitAggrFuncNode: (aggrFuncNode: AggrFunc) => T;
    visitValueNode: (valueNode: Value) => T;
    visitBinaryNode: (binaryNode: Binary) => T;
    visitIntervalNode: (intervalNode: Interval) => T;
    visitExpressionValueNode: (expressionValueNode: ExpressionValue) => T;
    visitExprListNode: (exprListNode: ExprList) => T;
    visitExtractNode: (extractNode: extract_func) => T;
    visitDateValueNode: (dateValueNode: DateValue) => T;
};

class RedShiftASTCollector implements NodeVisitor<DateValue | DateFilter | null> {
    res: DateFilter[] = [];

    visitDateValueNode(dateValueNode: DateValue): DateValue | DateFilter | null {
        // console.log("Visiting DateValueNode");
        return dateValueNode;
    };
    visitFunctionNode(functionNode: Function): DateValue | DateFilter | null {
        const type = functionNode.type;
        const name = functionNode.name;
        const args: ExprList = functionNode.args;
        const suffix = functionNode.suffix;
        const loc = functionNode.loc;
        const functionName = functionNode.name.name[0]?.value.toUpperCase(); // Normalize function name

        switch (functionName) {
            case 'CURRENT_DATE':
            case 'GETDATE':
                return {
                    type: "date_value",
                    numberOfPeriods: 0,
                    period: 'current',
                } as DateValue;

            case 'DATE_TRUNC': {
                // Assume it always has 2 arguments: period & DateValue/Function
                const period = (args.value[0] as ValueExpr<string>).value;
                const fieldArg: ExpressionValue = args.value[1];

                return processNode(fieldArg as Function, this) as DateValue;
            };
            case 'DATEADD': {
                const period = ((((args.value[0] as ColumnRefItem).column) as { expr: { value: string } }).expr as ValueExpr<string>).value;
                const offsetValue = (args.value[1] as ValueExpr<number>).value;
                const functionArg = processNode(args.value[2] as Function, this) as DateValue;

                return {
                    type: "date_value",
                    numberOfPeriods: functionArg.numberOfPeriods + offsetValue,
                    period: parsePeriod(period),
                } as DateValue;
            };

            default:
                console.warn(`Unhandled function: ${functionName}`);
                return null;
        };
    };
    visitColumnRefNode(columnRefNode: ColumnRefItem): DateValue | DateFilter | null {
        // console.log("Visiting ColumnRefNode");
        const type: string = columnRefNode.type;
        const table: string | null = columnRefNode.table;
        const column: string | { expr: ValueExpr } = columnRefNode.column;
        const options: ExprList = columnRefNode.options;
        const loc: LocationRange = columnRefNode.loc;

        if (typeof column === 'object' && 'expr' in column) {
            return {
                type: "date_value",
                numberOfPeriods: 0,
                period: 'all',
            } as DateValue;
        }
        // console.log("Reached ColumnRefNode without expr field dawg");
        return {
            type: "date_value",
            numberOfPeriods: 0,
            period: 'all',
        } as DateValue;
    };
    visitExtractNode(extractNode: extract_func): DateValue | DateFilter | null {
        // console.log("Visiting ExtractNode");
        const type = extractNode.type;
        const args = extractNode.args;
        const period: "current" | "days" | "weeks" | "months" | "quarters" | "years" | "all" = parsePeriod(args.field);
        const source = args.source;

        if (source.type == "column_ref") {
            return { ...processNode(source, this), period: period } as DateValue;
        }

        if (source.type == "function") {
            return processNode(source, this);
        }

        console.log("source in ExtractNode is not a column_ref or function");
        return null;
    };
    visitIntervalNode(intervalNode: Interval): DateValue | DateFilter | null {
        // console.log("Visiting Interval Node");
        const type: string = intervalNode.type;
        const expr = intervalNode.expr as ValueExpr<string>;
        const [numberOfPeriods, period] = expr.value.split(" ")
        return {
            type: "date_value",
            numberOfPeriods: parseInt(numberOfPeriods),
            period: parsePeriod(period),
        } as DateValue;;
    };
    visitBinaryNode(binaryNode: Binary): DateValue | DateFilter | null {
        // console.log("Visiting Binary Node");
        // Convert both children to DateValue
        var left = processNode(binaryNode.left, this) as DateValue;
        var right = processNode(binaryNode.right, this) as DateValue;

        // console.log("binaryNode: " + JSON.stringify(binaryNode, null, 4));
        console.log("left: " + JSON.stringify(left, null, 4));
        console.log("right: " + JSON.stringify(right, null, 4));
        console.log("binary operator: " + binaryNode.operator);
        console.log("Finished iteration RedShift\n\n\n");

        switch (binaryNode.operator) {
            // Both sides are normal DateValues
            case '+': {
                // console.log("I am in +");
                const leftPeriodIsAllOrCurrent = left.period == "all" || left.period == "current";
                return {
                    type: "date_value",
                    numberOfPeriods: left.numberOfPeriods + right.numberOfPeriods,
                    period: leftPeriodIsAllOrCurrent ? right.period : left.period,
                } as DateValue;
            };
            case '-': {
                // console.log("I am in -");
                const leftPeriodIsAllOrCurrent = left.period == "all" || left.period == "current";
                return {
                    type: "date_value",
                    numberOfPeriods: left.numberOfPeriods - right.numberOfPeriods,
                    period: leftPeriodIsAllOrCurrent ? right.period : left.period,
                } as DateValue;
            };
            case '=': {
                const leftPeriodIsAllOrCurrent = left.period == "all" || left.period == "current";

                var tmp: DateFilter = {
                    type: "current",
                    numberOfPeriods: 0,
                    period: (leftPeriodIsAllOrCurrent ? right.period : left.period) as "days" | "weeks" | "months" | "quarters" | "years",
                    field: "",
                };
                this.res.push(tmp);
                return null;
            };
            case '>':
            case '>=':
            case '<':
            case '<=': {

                console.log("Redshift getting activated");
                const leftPeriodIsAllOrCurrent = left.period == "all" || left.period == "current";
                const numberOfPeriod = leftPeriodIsAllOrCurrent ? right.numberOfPeriods : left.numberOfPeriods;
                const period = leftPeriodIsAllOrCurrent ? right.period : left.period;
                console.log("left period: " + left.period);
                console.log("right period: " + right.period);

                const types = new Map<number, string>([[0, "current"], [1, "next"], [-1, "previous"]]);
                const type = types.get(numberOfPeriod) ?? "last";

                var tmp = {
                    "type": type,
                    "numberOfPeriods": Math.abs(numberOfPeriod),
                    "period": period,
                    "field": "",
                } as DateFilter;

                this.res.push(tmp);
                return null;
            };

            case 'AND':
                return null;

            default:
                console.log("Unknown binary operator dawg");
                return null; // If not a date filter, return the original node
        };
    };
    visitExpressionValueNode(expressionValueNode: ExpressionValue): DateValue | DateFilter | null {
        return processNode(expressionValueNode, this);
    };
    visitParamNode(paramNode: Param): DateValue | DateFilter | null {
        return null;
    };
    visitCastNode(castNode: Cast): DateValue | DateFilter | null {
        return null;
    };
    visitAggrFuncNode(aggrFuncNode: AggrFunc): DateValue | DateFilter | null {
        return null;
    };
    visitValueNode(valueNode: Value): DateValue | DateFilter | null {
        return null;
    };
    visitExprListNode(exprListNode: ExprList): DateValue | DateFilter | null {
        return null;
    };
};

class BigQueryASTCollector implements NodeVisitor<DateValue | DateFilter | null> {
    res: DateFilter[] = [];

    visitDateValueNode(dateValueNode: DateValue): DateValue | DateFilter | null {
        // console.log("Visiting DateValueNode");
        return dateValueNode;
    };
    visitFunctionNode(functionNode: Function): DateValue | DateFilter | null {
        // TIMESTAMP_TRUNC(dateField, MONTH) = TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 30 DAY), MONTH)
        console.log("functionNode: " + JSON.stringify(functionNode, null, 4));
        const type = functionNode.type;
        const name = functionNode.name;
        const args: ExprList = functionNode.args;
        const suffix = functionNode.suffix;
        const loc = functionNode.loc;
        const functionName = functionNode.name?.name[0]?.value?.toUpperCase() ?? functionNode.name?.schema?.value?.toUpperCase(); // Normalize function name

        switch (functionName) {
            case 'CURRENT_DATE':
            case 'GETDATE':
            case 'CURRENT_TIMESTAMP':
                return {
                    type: "date_value",
                    numberOfPeriods: 0,
                    period: 'current',
                } as DateValue;

            case 'DATE_TRUNC': {
                // Assume it always has 2 arguments: period & DateValue/Function
                const period = (args.value[0] as ValueExpr<string>).value;
                const fieldArg: ExpressionValue = args.value[1];

                return processNode(fieldArg as Function, this) as DateValue;
            };
            case 'TIMESTAMP_TRUNC': {
                console.log("value0: " + JSON.stringify(args.value[0], null, 4));

                const dateValue = processNode(args.value[0], this) as DateValue;
                const period = parsePeriod(((args.value[1] as ColumnRefItem).column as { expr: { type: string, value: string } }).expr.value);
                // console.log("dateValue: " + JSON.stringify(dateValue, null, 4));
                // console.log("period: " + period);

                // For double ColumnRef
                const dateValueWithNumberOfPeriods = { ...dateValue, numberOfPeriods: dateValue.numberOfPeriods ?? 0 } as DateValue;
                const tmp = getTruncatedDate(dateValueWithNumberOfPeriods, period);

                console.log("TIMESTAMP_TRUNC: " + JSON.stringify(tmp, null, 4));
                return tmp;
            };
            // TIMESTAMP_TRUNC(dateField, MONTH) = TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 30 DAY), MONTH)

            case 'DATE_SUB':
            case 'TIMESTAMP_SUB': {
                // SELECT * FROM transactions WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)
                const functionArg = processNode(args.value[0] as Function, this) as DateValue;
                // const period = ((((args.value[1] as ColumnRefItem).column) as { expr: { value: string } }).expr as ValueExpr<string>).value;
                const offsetValue = ((args.value[1] as Interval).expr as ValueExpr<number>).value;
                const period = parsePeriod((args.value[1] as Interval).unit);

                const tmp =  {
                    type: "date_value",
                    numberOfPeriods: functionArg.numberOfPeriods - offsetValue,
                    period: parsePeriod(period),
                } as DateValue;
                console.log("TIMESTAMP_SUB: " + JSON.stringify(tmp, null, 4));
                return tmp;
            };
            case 'PARSE_TIMESTAMP': {
                // PARSE_TIMESTAMP(\"%Y-%M-%D\", transaction_date) = PARSE_TIMESTAMP(\"%Y-%M-%D\", CURRENT_DATE())
                const functionArg = processNode(args.value[1] as Function, this) as DateValue;
                const timeFormat = (args.value[0] as ValueExpr<string>).value;
                if (timeFormat.includes("D")) {
                    return { ...functionArg, period: "days" } as DateValue;
                }
                if (timeFormat.includes("M")) {
                    return { ...functionArg, period: "months" } as DateValue;
                }
                if (timeFormat.includes("Y")) {
                    return { ...functionArg, period: "years" } as DateValue;
                }
            };
            default:
                console.warn(`Unhandled function: ${functionName}`);
                return null;
        };
    };
    visitColumnRefNode(columnRefNode: ColumnRefItem): DateValue | DateFilter | null {
        // console.log("Visiting ColumnRefNode");
        const type: string = columnRefNode.type;
        const table: string | null = columnRefNode.table;
        const column: string | { expr: ValueExpr } = columnRefNode.column;
        const options: ExprList = columnRefNode.options;
        const loc: LocationRange = columnRefNode.loc;

        if (typeof column === 'object' && 'expr' in column) {
            return {
                type: "date_value",
                numberOfPeriods: 0,
                period: 'all',
            } as DateValue;
        }
        // console.log("Reached ColumnRefNode without expr field dawg");
        return {
            type: "date_value",
            numberOfPeriods: 0,
            period: 'all',
        } as DateValue;
    };
    visitExtractNode(extractNode: extract_func): DateValue | DateFilter | null {
        // console.log("Visiting ExtractNode");
        const type = extractNode.type;
        const args = extractNode.args;
        const period: "current" | "days" | "weeks" | "months" | "quarters" | "years" | "all" = parsePeriod(args.field);
        const source = args.source;

        if (source.type == "column_ref") {
            return { ...processNode(source, this), period: period } as DateValue;
        }

        if (source.type == "function") {
            return processNode(source, this);
        }

        console.log("source in ExtractNode is not a column_ref or function");
        return null;
    };
    visitIntervalNode(intervalNode: Interval): DateValue | DateFilter | null {
        // console.log("Visiting Interval Node");
        const type: string = intervalNode.type;
        const expr = intervalNode.expr as ValueExpr<string>;
        const [numberOfPeriods, period] = expr.value.split(" ")
        return {
            type: "date_value",
            numberOfPeriods: parseInt(numberOfPeriods),
            period: parsePeriod(period),
        } as DateValue;;
    };
    visitBinaryNode(binaryNode: Binary): DateValue | DateFilter | null {
        // TIMESTAMP_TRUNC(dateField, MONTH) = TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 30 DAY), MONTH)

        var left = processNode(binaryNode.left, this) as DateValue;
        var right = processNode(binaryNode.right, this) as DateValue;

        // console.log("binaryNode: " + JSON.stringify(binaryNode, null, 4));
        console.log("left: " + JSON.stringify(left, null, 4));
        console.log("right: " + JSON.stringify(right, null, 4));
        console.log("binary operator: " + binaryNode.operator);
        console.log("Finished iteration BigQuery\n\n\n");

        switch (binaryNode.operator) {
            // Both sides are normal DateValues
            case '+': {
                // console.log("I am in +");
                const leftPeriodIsAllOrCurrent = left.period == "all" || left.period == "current";
                return {
                    type: "date_value",
                    numberOfPeriods: left.numberOfPeriods + right.numberOfPeriods,
                    period: leftPeriodIsAllOrCurrent ? right.period : left.period,
                } as DateValue;
            };
            case '-': {
                // console.log("I am in -");
                const leftPeriodIsAllOrCurrent = left.period == "all" || left.period == "current";
                return {
                    type: "date_value",
                    numberOfPeriods: left.numberOfPeriods - right.numberOfPeriods,
                    period: leftPeriodIsAllOrCurrent ? right.period : left.period,
                } as DateValue;
            };
            case '=': {
                const types = new Map<number, string>([[0, "current"], [1, "next"], [-1, "previous"]]);
                const type = types.get(right.numberOfPeriods) ?? "last";

                const leftPeriodIsAllOrCurrent = left.period == "all" || left.period == "current";
                var tmp: DateFilter = {
                    type: type as "current" | "next" | "previous" | "last",
                    numberOfPeriods: Math.abs(right.numberOfPeriods),
                    period: (leftPeriodIsAllOrCurrent ? right.period : left.period) as "days" | "weeks" | "months" | "quarters" | "years",
                    field: "",
                };
                this.res.push(tmp);
                return null;
            };
            case '>':
            case '>=':
            case '<':
            case '<=': {
                const leftPeriodIsAllOrCurrent = left.period == "all" || left.period == "current";
                const numberOfPeriod = leftPeriodIsAllOrCurrent ? right.numberOfPeriods : left.numberOfPeriods;
                const period = leftPeriodIsAllOrCurrent ? right.period : left.period;
                console.log("left period: " + left.period);
                console.log("right period: " + right.period);

                const types = new Map<number, string>([[0, "current"], [1, "next"], [-1, "previous"]]);
                const type = types.get(numberOfPeriod) ?? "last";

                var tmp = {
                    "type": type,
                    "numberOfPeriods": Math.abs(numberOfPeriod),
                    "period": period,
                    "field": "",
                } as DateFilter;

                this.res.push(tmp);
                return null;
            };

            case 'AND':
                return null;

            default:
                console.log("Unknown binary operator dawg");
                return null; // If not a date filter, return the original node
        };
    };
    visitExpressionValueNode(expressionValueNode: ExpressionValue): DateValue | DateFilter | null {
        return processNode(expressionValueNode, this);
    };
    visitParamNode(paramNode: Param): DateValue | DateFilter | null {
        return null;
    };
    visitCastNode(castNode: Cast): DateValue | DateFilter | null {
        return null;
    };
    visitAggrFuncNode(aggrFuncNode: AggrFunc): DateValue | DateFilter | null {
        return null;
    };
    visitValueNode(valueNode: Value): DateValue | DateFilter | null {
        return null;
    };
    visitExprListNode(exprListNode: ExprList): DateValue | DateFilter | null {
        return null;
    };
};

class MySQLASTCollector implements NodeVisitor<DateValue | DateFilter | null> {
    res: DateFilter[] = [];

    visitDateValueNode(dateValueNode: DateValue): DateValue | DateFilter | null {
        // console.log("Visiting DateValueNode");
        return dateValueNode;
    };
    visitFunctionNode(functionNode: Function): DateValue | DateFilter | null {
        // TIMESTAMP_TRUNC(dateField, MONTH) = TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 30 DAY), MONTH)
        console.log("functionNode: " + JSON.stringify(functionNode, null, 4));
        const type = functionNode.type;
        const name = functionNode.name;
        const args: ExprList = functionNode.args;
        const suffix = functionNode.suffix;
        const loc = functionNode.loc;
        const functionName = functionNode.name?.name[0]?.value?.toUpperCase() ?? functionNode.name?.schema?.value?.toUpperCase(); // Normalize function name

        switch (functionName) {
            case 'CURRENT_DATE':
            case 'CURDATE':
            case 'GETDATE':
            case 'CURRENT_TIMESTAMP':
            case 'NOW':
                return {
                    type: "date_value",
                    numberOfPeriods: 0,
                    period: 'current',
                } as DateValue;

            case 'DATE_TRUNC': {
                // Assume it always has 2 arguments: period & DateValue/Function
                const period = (args.value[0] as ValueExpr<string>).value;
                const fieldArg: ExpressionValue = args.value[1];

                return processNode(fieldArg as Function, this) as DateValue;
            };
            case 'TIMESTAMP_TRUNC': {
                const dateValue = processNode(args.value[0] as Function, this) as DateValue;
                const period = parsePeriod(((args.value[1] as ColumnRefItem).column as { expr: { type: string, value: string } }).expr.value);
                return getTruncatedDate(dateValue, period);
            };
            // TIMESTAMP_TRUNC(dateField, MONTH) = TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 30 DAY), MONTH)
            case 'DATEADD': {
                const period = ((((args.value[0] as ColumnRefItem).column) as { expr: { value: string } }).expr as ValueExpr<string>).value;
                const offsetValue = (args.value[1] as ValueExpr<number>).value;
                const functionArg = processNode(args.value[2] as Function, this) as DateValue;

                return {
                    type: "date_value",
                    numberOfPeriods: functionArg.numberOfPeriods + offsetValue,
                    period: parsePeriod(period),
                } as DateValue;
            };

            case 'DATE_SUB':
            case 'TIMESTAMP_SUB': {
                // SELECT * FROM transactions WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)
                const functionArg = processNode(args.value[0] as Function, this) as DateValue;
                // const period = ((((args.value[1] as ColumnRefItem).column) as { expr: { value: string } }).expr as ValueExpr<string>).value;
                const offsetValue = ((args.value[1] as Interval).expr as ValueExpr<number>).value;
                const period = parsePeriod((args.value[1] as Interval).unit);

                return {
                    type: "date_value",
                    numberOfPeriods: functionArg.numberOfPeriods - offsetValue,
                    period: parsePeriod(period),
                } as DateValue;
            };
            case 'PARSE_TIMESTAMP': {
                // PARSE_TIMESTAMP(\"%Y-%M-%D\", transaction_date) = PARSE_TIMESTAMP(\"%Y-%M-%D\", CURRENT_DATE())
                const functionArg = processNode(args.value[1] as Function, this) as DateValue;
                const timeFormat = (args.value[0] as ValueExpr<string>).value;
                if (timeFormat.includes("D")) {
                    return { ...functionArg, period: "days" } as DateValue;
                }
                if (timeFormat.includes("M")) {
                    return { ...functionArg, period: "months" } as DateValue;
                }
                if (timeFormat.includes("Y")) {
                    return { ...functionArg, period: "years" } as DateValue;
                }
            };
            case 'YEAR': {
                const dateValue = processNode(args.value[0] as Function, this) as DateValue;
                return getTruncatedDate(dateValue, 'years');
            }
            case 'MONTH': {
                const dateValue = processNode(args.value[0] as Function, this) as DateValue;
                return getTruncatedDate(dateValue, 'months');
            }

            default:
                console.warn(`Unhandled function: ${functionName}`);
                return null;
        };
    };
    visitColumnRefNode(columnRefNode: ColumnRefItem): DateValue | DateFilter | null {
        // console.log("Visiting ColumnRefNode");
        const type: string = columnRefNode.type;
        const table: string | null = columnRefNode.table;
        const column: string | { expr: ValueExpr } = columnRefNode.column;
        const options: ExprList = columnRefNode.options;
        const loc: LocationRange = columnRefNode.loc;

        if (typeof column === 'object' && 'expr' in column) {
            return {
                type: "date_value",
                numberOfPeriods: 0,
                period: 'all',
            } as DateValue;
        }
        // console.log("Reached ColumnRefNode without expr field dawg");
        return {
            type: "date_value",
            numberOfPeriods: 0,
            period: 'all',
        } as DateValue;
    };
    visitExtractNode(extractNode: extract_func): DateValue | DateFilter | null {
        // console.log("Visiting ExtractNode");
        const type = extractNode.type;
        const args = extractNode.args;
        const period: "current" | "days" | "weeks" | "months" | "quarters" | "years" | "all" = parsePeriod(args.field);
        const source = args.source;

        if (source.type == "column_ref") {
            return { ...processNode(source, this), period: period } as DateValue;
        }

        if (source.type == "function") {
            return processNode(source, this);
        }

        console.log("source in ExtractNode is not a column_ref or function");
        return null;
    };
    visitIntervalNode(intervalNode: Interval): DateValue | DateFilter | null {
        // console.log("Visiting Interval Node");
        const type: string = intervalNode.type;
        const numberOfPeriods: number = (intervalNode.expr as ValueExpr<number>).value;
        const period = intervalNode.unit;
        return {
            type: "date_value",
            numberOfPeriods: numberOfPeriods,
            period: parsePeriod(period),
        } as DateValue;;
    };
    visitBinaryNode(binaryNode: Binary): DateValue | DateFilter | null {
        // TIMESTAMP_TRUNC(dateField, MONTH) = TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 30 DAY), MONTH)

        var left = processNode(binaryNode.left, this) as DateValue;
        var right = processNode(binaryNode.right, this) as DateValue;

        // console.log("binaryNode: " + JSON.stringify(binaryNode, null, 4));
        console.log("left: " + JSON.stringify(left, null, 4));
        console.log("right: " + JSON.stringify(right, null, 4));
        console.log("binary operator: " + binaryNode.operator);
        console.log("Finished iteration MySQL\n\n\n");

        switch (binaryNode.operator) {
            // Both sides are normal DateValues
            case '+': {
                // console.log("I am in +");
                const leftPeriodIsAllOrCurrent = left.period == "all" || left.period == "current";
                return {
                    type: "date_value",
                    numberOfPeriods: left.numberOfPeriods + right.numberOfPeriods,
                    period: leftPeriodIsAllOrCurrent ? right.period : left.period,
                } as DateValue;
            };
            case '-': {
                // console.log("I am in -");
                const leftPeriodIsAllOrCurrent = left.period == "all" || left.period == "current";
                return {
                    type: "date_value",
                    numberOfPeriods: left.numberOfPeriods - right.numberOfPeriods,
                    period: leftPeriodIsAllOrCurrent ? right.period : left.period,
                } as DateValue;
            };
            case '=': {
                const leftPeriodIsAllOrCurrent = left.period == "all" || left.period == "current";
                console.log("in =: right.numberofperiods is " + right.numberOfPeriods);
                var tmp: DateFilter = {
                    type: "current",
                    numberOfPeriods: 0, // TODO: inaccurate
                    period: (leftPeriodIsAllOrCurrent ? right.period : left.period) as "days" | "weeks" | "months" | "quarters" | "years",
                    field: "",
                };
                this.res.push(tmp);
                return null;
            };
            case '>':
            case '>=':
            case '<':
            case '<=': {
                const leftPeriodIsAllOrCurrent = left.period == "all" || left.period == "current";
                const numberOfPeriod = leftPeriodIsAllOrCurrent ? right.numberOfPeriods : left.numberOfPeriods;
                const period = leftPeriodIsAllOrCurrent ? right.period : left.period;
                console.log("left period: " + left.period);
                console.log("right period: " + right.period);

                const types = new Map<number, string>([[0, "current"], [1, "next"], [-1, "previous"]]);
                const type = types.get(numberOfPeriod) ?? "last";

                var tmp = {
                    "type": type,
                    "numberOfPeriods": Math.abs(numberOfPeriod),
                    "period": period,
                    "field": "",
                } as DateFilter;

                this.res.push(tmp);
                return null;
            };

            case 'AND':
                return null;

            default:
                console.log("Unknown binary operator dawg");
                return null; // If not a date filter, return the original node
        };
    };
    visitExpressionValueNode(expressionValueNode: ExpressionValue): DateValue | DateFilter | null {
        return processNode(expressionValueNode, this);
    };
    visitParamNode(paramNode: Param): DateValue | DateFilter | null {
        return null;
    };
    visitCastNode(castNode: Cast): DateValue | DateFilter | null {
        return null;
    };
    visitAggrFuncNode(aggrFuncNode: AggrFunc): DateValue | DateFilter | null {
        return null;
    };
    visitValueNode(valueNode: Value): DateValue | DateFilter | null {
        return null;
    };
    visitExprListNode(exprListNode: ExprList): DateValue | DateFilter | null {
        return null;
    };
};

class PostgreSQLASTCollector implements NodeVisitor<DateValue | DateFilter | null> {
    res: DateFilter[] = [];

    visitDateValueNode(dateValueNode: DateValue): DateValue | DateFilter | null {
        // console.log("Visiting DateValueNode");
        return dateValueNode;
    };
    visitFunctionNode(functionNode: Function): DateValue | DateFilter | null {
        // TIMESTAMP_TRUNC(dateField, MONTH) = TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 30 DAY), MONTH)
        console.log("functionNode: " + JSON.stringify(functionNode, null, 4));
        const type = functionNode.type;
        const name = functionNode.name;
        const args: ExprList = functionNode.args;
        const suffix = functionNode.suffix;
        const loc = functionNode.loc;
        const functionName = functionNode.name?.name[0]?.value?.toUpperCase() ?? functionNode.name?.schema?.value?.toUpperCase(); // Normalize function name

        switch (functionName) {
            case 'CURRENT_DATE':
            case 'CURDATE':
            case 'GETDATE':
            case 'CURRENT_TIMESTAMP':
            case 'NOW':
                return {
                    type: "date_value",
                    numberOfPeriods: 0,
                    period: 'current',
                } as DateValue;

            case 'DATE_TRUNC': {
                // Assume it always has 2 arguments: period & DateValue/Function
                const period = parsePeriod((args.value[0] as ValueExpr<string>).value);
                const fieldArg: ExpressionValue = args.value[1];

                const dateValue = processNode(fieldArg as Function, this) as DateValue;
                return { ...dateValue, period: period } as DateValue;
            };
            case 'TIMESTAMP_TRUNC': {
                const dateValue = processNode(args.value[0] as Function, this) as DateValue;
                const period = parsePeriod(((args.value[1] as ColumnRefItem).column as { expr: { type: string, value: string } }).expr.value);
                return getTruncatedDate(dateValue, period);
            };
            // TIMESTAMP_TRUNC(dateField, MONTH) = TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 30 DAY), MONTH)

            case 'DATE_SUB':
            case 'TIMESTAMP_SUB': {
                // SELECT * FROM transactions WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)
                const functionArg = processNode(args.value[0] as Function, this) as DateValue;
                // const period = ((((args.value[1] as ColumnRefItem).column) as { expr: { value: string } }).expr as ValueExpr<string>).value;
                const offsetValue = ((args.value[1] as Interval).expr as ValueExpr<number>).value;
                const period = parsePeriod((args.value[1] as Interval).unit);

                return {
                    type: "date_value",
                    numberOfPeriods: functionArg.numberOfPeriods - offsetValue,
                    period: parsePeriod(period),
                } as DateValue;
            };
            case 'PARSE_TIMESTAMP': {
                // PARSE_TIMESTAMP(\"%Y-%M-%D\", transaction_date) = PARSE_TIMESTAMP(\"%Y-%M-%D\", CURRENT_DATE())
                const functionArg = processNode(args.value[1] as Function, this) as DateValue;
                const timeFormat = (args.value[0] as ValueExpr<string>).value;
                if (timeFormat.includes("D")) {
                    return { ...functionArg, period: "days" } as DateValue;
                }
                if (timeFormat.includes("M")) {
                    return { ...functionArg, period: "months" } as DateValue;
                }
                if (timeFormat.includes("Y")) {
                    return { ...functionArg, period: "years" } as DateValue;
                }
            };
            case 'YEAR': {
                const dateValue = processNode(args.value[0] as Function, this) as DateValue;
                return getTruncatedDate(dateValue, 'years');
            }
            case 'MONTH': {
                const dateValue = processNode(args.value[0] as Function, this) as DateValue;
                return getTruncatedDate(dateValue, 'months');
            }

            default:
                console.warn(`Unhandled function: ${functionName}`);
                return null;
        };
    };
    visitColumnRefNode(columnRefNode: ColumnRefItem): DateValue | DateFilter | null {
        // console.log("Visiting ColumnRefNode");
        const type: string = columnRefNode.type;
        const table: string | null = columnRefNode.table;
        const column: string | { expr: ValueExpr } = columnRefNode.column;
        const options: ExprList = columnRefNode.options;
        const loc: LocationRange = columnRefNode.loc;

        if (typeof column === 'object' && 'expr' in column) {
            return {
                type: "date_value",
                numberOfPeriods: 0,
                period: 'all',
            } as DateValue;
        }
        // console.log("Reached ColumnRefNode without expr field dawg");
        return {
            type: "date_value",
            numberOfPeriods: 0,
            period: 'all',
        } as DateValue;
    };
    visitExtractNode(extractNode: extract_func): DateValue | DateFilter | null {
        // console.log("Visiting ExtractNode");
        const type = extractNode.type;
        const args = extractNode.args;
        const period: "current" | "days" | "weeks" | "months" | "quarters" | "years" | "all" = parsePeriod(args.field);
        const source = args.source;

        if (source.type == "column_ref") {
            return { ...processNode(source, this), period: period } as DateValue;
        }

        if (source.type == "function") {
            return processNode(source, this);
        }

        if (source.type == "binary_expr") {
            const dateValue = processNode(source, this) as DateValue;
            return getTruncatedDate(dateValue, parsePeriod(args.field));
        }

        console.log("source in ExtractNode is not a column_ref or function");
        return null;
    };
    visitIntervalNode(intervalNode: Interval): DateValue | DateFilter | null {
        console.log("Visiting Interval Node");
        const type: string = intervalNode.type;
        const [numberOfPeriods, period] = (intervalNode.expr as ValueExpr<string>).value.split(" ");
        return {
            type: "date_value",
            numberOfPeriods: parseInt(numberOfPeriods),
            period: parsePeriod(period),
        } as DateValue;;
    };
    visitBinaryNode(binaryNode: Binary): DateValue | DateFilter | null {
        // TIMESTAMP_TRUNC(dateField, MONTH) = TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 30 DAY), MONTH)

        var left = processNode(binaryNode.left, this) as DateValue;
        var right = processNode(binaryNode.right, this) as DateValue;

        // console.log("binaryNode: " + JSON.stringify(binaryNode, null, 4));
        console.log("left: " + JSON.stringify(left, null, 4));
        console.log("right: " + JSON.stringify(right, null, 4));
        console.log("binary operator: " + binaryNode.operator);
        console.log("Finished iteration PostgreSQL\n\n\n");

        switch (binaryNode.operator) {
            // Both sides are normal DateValues
            case '+': {
                // console.log("I am in +");
                const leftPeriodIsAllOrCurrent = left.period == "all" || left.period == "current";
                return {
                    type: "date_value",
                    numberOfPeriods: left.numberOfPeriods + right.numberOfPeriods,
                    period: leftPeriodIsAllOrCurrent ? right.period : left.period,
                } as DateValue;
            };
            case '-': {
                // console.log("I am in -");
                const leftPeriodIsAllOrCurrent = left.period == "all" || left.period == "current";
                return {
                    type: "date_value",
                    numberOfPeriods: left.numberOfPeriods - right.numberOfPeriods,
                    period: leftPeriodIsAllOrCurrent ? right.period : left.period,
                } as DateValue;
            };
            case '=': {
                const leftPeriodIsAllOrCurrent = left.period == "all" || left.period == "current";
                console.log("in =: right.numberofperiods is " + right.numberOfPeriods);

                const types = new Map<number, string>([[0, "current"], [1, "next"], [-1, "previous"]]);
                const type = types.get(right.numberOfPeriods) ?? "last";

                var tmp: DateFilter = {
                    type: type as "current" | "next" | "previous" | "last",
                    numberOfPeriods: Math.abs(right.numberOfPeriods),
                    period: (leftPeriodIsAllOrCurrent ? right.period : left.period) as "days" | "weeks" | "months" | "quarters" | "years",
                    field: "",
                };
                this.res.push(tmp);
                return null;
            };
            case '>':
            case '>=':
            case '<':
            case '<=': {
                const leftPeriodIsAllOrCurrent = left.period == "all" || left.period == "current";
                const numberOfPeriod = leftPeriodIsAllOrCurrent ? right.numberOfPeriods : left.numberOfPeriods;
                const period = leftPeriodIsAllOrCurrent ? right.period : left.period;
                console.log("left period: " + left.period);
                console.log("right period: " + right.period);

                const types = new Map<number, string>([[0, "current"], [1, "next"], [-1, "previous"]]);
                const type = types.get(numberOfPeriod) ?? "last";

                var tmp = {
                    "type": type,
                    "numberOfPeriods": Math.abs(numberOfPeriod),
                    "period": period,
                    "field": "",
                } as DateFilter;

                this.res.push(tmp);
                return null;
            };

            case 'AND':
                return null;

            default:
                console.log("Unknown binary operator dawg");
                return null; // If not a date filter, return the original node
        };
    };
    visitExpressionValueNode(expressionValueNode: ExpressionValue): DateValue | DateFilter | null {
        return processNode(expressionValueNode, this);
    };
    visitParamNode(paramNode: Param): DateValue | DateFilter | null {
        return null;
    };
    visitCastNode(castNode: Cast): DateValue | DateFilter | null {
        return null;
    };
    visitAggrFuncNode(aggrFuncNode: AggrFunc): DateValue | DateFilter | null {
        return null;
    };
    visitValueNode(valueNode: Value): DateValue | DateFilter | null {
        return null;
    };
    visitExprListNode(exprListNode: ExprList): DateValue | DateFilter | null {
        return null;
    };
};

class SnowflakeASTCollector implements NodeVisitor<DateValue | DateFilter | null> {
    res: DateFilter[] = [];

    visitDateValueNode(dateValueNode: DateValue): DateValue | DateFilter | null {
        // console.log("Visiting DateValueNode");
        return dateValueNode;
    };
    visitFunctionNode(functionNode: Function): DateValue | DateFilter | null {
        // TIMESTAMP_TRUNC(dateField, MONTH) = TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 30 DAY), MONTH)
        // console.log("functionNode: " + JSON.stringify(functionNode, null, 4));
        const type = functionNode.type;
        const name = functionNode.name;
        const args: ExprList = functionNode.args;
        const suffix = functionNode.suffix;
        const loc = functionNode.loc;
        const functionName = functionNode.name?.name[0]?.value?.toUpperCase() ?? functionNode.name?.schema?.value?.toUpperCase(); // Normalize function name

        // console.log("functionName: " + functionName);
        switch (functionName) {
            case 'CURRENT_DATE':
            case 'CURDATE':
            case 'GETDATE':
            case 'CURRENT_TIMESTAMP':
            case 'NOW':
                return {
                    type: "date_value",
                    numberOfPeriods: 0,
                    period: 'current',
                } as DateValue;

            case 'DATE_TRUNC': {
                // Assume it always has 2 arguments: period & DateValue/Function
                const period = (args.value[0] as ValueExpr<string>).value;
                const fieldArg: ExpressionValue = args.value[1];

                return processNode(fieldArg as Function, this) as DateValue;
            };
            case 'TIMESTAMP_TRUNC': {
                const dateValue = processNode(args.value[0] as Function, this) as DateValue;
                const period = parsePeriod(((args.value[1] as ColumnRefItem).column as { expr: { type: string, value: string } }).expr.value);
                return getTruncatedDate(dateValue, period);
            };
            // SELECT * FROM transactions WHERE transaction_date >= DATEADD(YEAR, -1, CURRENT_DATE())
            case 'DATEADD': {
                console.log("YO we hit DATEADD!");
                const period = parsePeriod(((args.value[0] as ColumnRefItem).column) as string);
                const offsetValue = (args.value[1] as ValueExpr<number>).value;
                const functionArg = processNode(args.value[2] as Function, this) as DateValue;

                return {
                    type: "date_value",
                    numberOfPeriods: functionArg.numberOfPeriods + offsetValue,
                    period: parsePeriod(period),
                } as DateValue;
            };

            case 'DATE_SUB':
            case 'TIMESTAMP_SUB': {
                // SELECT * FROM transactions WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)
                const functionArg = processNode(args.value[0] as Function, this) as DateValue;
                // const period = ((((args.value[1] as ColumnRefItem).column) as { expr: { value: string } }).expr as ValueExpr<string>).value;
                const offsetValue = ((args.value[1] as Interval).expr as ValueExpr<number>).value;
                const period = parsePeriod((args.value[1] as Interval).unit);

                return {
                    type: "date_value",
                    numberOfPeriods: functionArg.numberOfPeriods - offsetValue,
                    period: parsePeriod(period),
                } as DateValue;
            };
            case 'PARSE_TIMESTAMP': {
                // PARSE_TIMESTAMP(\"%Y-%M-%D\", transaction_date) = PARSE_TIMESTAMP(\"%Y-%M-%D\", CURRENT_DATE())
                const functionArg = processNode(args.value[1] as Function, this) as DateValue;
                const timeFormat = (args.value[0] as ValueExpr<string>).value;
                if (timeFormat.includes("D")) {
                    return { ...functionArg, period: "days" } as DateValue;
                }
                if (timeFormat.includes("M")) {
                    return { ...functionArg, period: "months" } as DateValue;
                }
                if (timeFormat.includes("Y")) {
                    return { ...functionArg, period: "years" } as DateValue;
                }
            };
            case 'YEAR': {
                const dateValue = processNode(args.value[0] as Function, this) as DateValue;
                return getTruncatedDate(dateValue, 'years');
            }
            case 'MONTH': {
                const dateValue = processNode(args.value[0] as Function, this) as DateValue;
                return getTruncatedDate(dateValue, 'months');
            }
            case 'LAST_DAY': {
                const dateValue = processNode(args.value[0] as Function, this) as DateValue;
                return { ...dateValue, numberOfPeriods: dateValue.numberOfPeriods + 1 } as DateValue;
            }

            default:
                console.warn(`Unhandled function: ${functionName}`);
                return null;
        };
    };
    visitColumnRefNode(columnRefNode: ColumnRefItem): DateValue | null {
        console.log("Visiting ColumnRef");
        const type: string = columnRefNode.type;
        const table: string | null = columnRefNode.table;
        const column: string | { expr: ValueExpr } = columnRefNode.column;
        const options: ExprList = columnRefNode.options;
        const loc: LocationRange = columnRefNode.loc;

        if (typeof column === 'object' && 'expr' in column) {
            return {
                type: "date_value",
                numberOfPeriods: 0,
                period: 'all',
            } as DateValue;
        }
        // console.log("Reached ColumnRefNode without expr field dawg");
        return {
            type: "date_value",
            numberOfPeriods: 0,
            period: 'all',
        } as DateValue;
    };
    visitExtractNode(extractNode: extract_func): DateValue | DateFilter | null {
        // console.log("Visiting ExtractNode");
        const type = extractNode.type;
        const args = extractNode.args;
        const period: "current" | "days" | "weeks" | "months" | "quarters" | "years" | "all" = parsePeriod(args.field);
        const source = args.source;

        if (source.type == "column_ref") {
            return { ...processNode(source, this), period: period } as DateValue;
        }

        if (source.type == "function") {
            return processNode(source, this);
        }

        if (source.type == "binary_expr") {
            const dateValue = processNode(source, this) as DateValue;
            return getTruncatedDate(dateValue, parsePeriod(args.field));
        }

        console.log("source in ExtractNode is not a column_ref or function");
        return null;
    };
    visitIntervalNode(intervalNode: Interval): DateValue | DateFilter | null {
        console.log("Visiting Interval");
        const type: string = intervalNode.type;
        // const numberOfPeriods: number = (intervalNode.expr as ValueExpr<number>).value;
        // const period = intervalNode.unit;

        const exprValues = ((intervalNode.expr) as ValueExpr<string>).value.split(" ");
        const numberOfPeriods = parseInt(exprValues[0]);
        const period = parsePeriod(exprValues[1]);
        return {
            type: "date_value",
            numberOfPeriods: numberOfPeriods,
            period: parsePeriod(period),
        } as DateValue;
    };
    visitBinaryNode(binaryNode: Binary): DateValue | DateFilter | null {
        console.log("binary operator: " + binaryNode.operator);

        // transaction_date BETWEEN DATE_TRUNC('MONTH', CURRENT_DATE() - INTERVAL '1 MONTH') AND LAST_DAY(DATE_TRUNC('MONTH', CURRENT_DATE() - INTERVAL '1 MONTH'));
        if (binaryNode.operator == "BETWEEN") {
            const left = processNode(binaryNode.left as ColumnRefItem, this) as DateValue;
            console.log("left: " + JSON.stringify(left, null, 4));

            const range = binaryNode.right as ExprList;
            const start = processNode(range.value[0] as Function, this) as DateValue;
            // const end = processNode(range.value[1] as Function, this) as DateValue;

            const types = new Map<number, string>([[0, "current"], [1, "next"], [-1, "previous"]]);
            const startType = types.get(start.numberOfPeriods) ?? "last";

            var leftBound = {
                "type": startType,
                "numberOfPeriods": Math.abs(start.numberOfPeriods),
                "period": start.period,
                "field": "",
            } as DateFilter;

            // var rightBound = {
            //     "type": "previous",
            //     "numberOfPeriods": Math.abs(end.numberOfPeriods),
            //     "period": end.period,
            //     "field": "",
            // } as DateFilter;

            this.res.push(leftBound);
            // this.res.push(rightBound);
            return null;
        }

        // Normal binary operators
        const left = processNode(binaryNode.left, this) as DateValue;
        console.log("left: " + JSON.stringify(left, null, 4));

        const right = processNode(binaryNode.right, this) as DateValue;
        console.log("right: " + JSON.stringify(right, null, 4));

        // console.log("binaryNode: " + JSON.stringify(binaryNode, null, 4));

        console.log("Finished iteration Snowflake\n\n\n");

        switch (binaryNode.operator) {
            // Both sides are normal DateValues
            case '+': {
                // console.log("I am in +");
                const leftPeriodIsAllOrCurrent = left.period == "all" || left.period == "current";
                return {
                    type: "date_value",
                    numberOfPeriods: left.numberOfPeriods + right.numberOfPeriods,
                    period: leftPeriodIsAllOrCurrent ? right.period : left.period,
                } as DateValue;
            };
            case '-': {
                // console.log("I am in -");
                const leftPeriodIsAllOrCurrent = left.period == "all" || left.period == "current";
                return {
                    type: "date_value",
                    numberOfPeriods: left.numberOfPeriods - right.numberOfPeriods,
                    period: leftPeriodIsAllOrCurrent ? right.period : left.period,
                } as DateValue;
            };
            case '=': {
                const leftPeriodIsAllOrCurrent = left.period == "all" || left.period == "current";
                console.log("in =: right.numberofperiods is " + right.numberOfPeriods);
                var tmp: DateFilter = {
                    type: "current",
                    numberOfPeriods: 0, // TODO: inaccurate
                    period: (leftPeriodIsAllOrCurrent ? right.period : left.period) as "days" | "weeks" | "months" | "quarters" | "years",
                    field: "",
                };
                this.res.push(tmp);
                return null;
            };
            case '>':
            case '>=':
            case '<':
            case '<=': {
                const leftPeriodIsAllOrCurrent = left.period == "all" || left.period == "current";
                const numberOfPeriod = leftPeriodIsAllOrCurrent ? right.numberOfPeriods : left.numberOfPeriods;
                const period = leftPeriodIsAllOrCurrent ? right.period : left.period;
                console.log("left period: " + left.period);
                console.log("right period: " + right.period);

                const types = new Map<number, string>([[0, "current"], [1, "next"], [-1, "previous"]]);
                const type = types.get(numberOfPeriod) ?? "last";

                var tmp = {
                    "type": type,
                    "numberOfPeriods": Math.abs(numberOfPeriod),
                    "period": period,
                    "field": "",
                } as DateFilter;

                this.res.push(tmp);
                return null;
            };

            case 'AND':
                return null;


            default:
                console.log("Unknown binary operator dawg");
                return null; // If not a date filter, return the original node
        };
    };
    visitExpressionValueNode(expressionValueNode: ExpressionValue): DateValue | DateFilter | null {
        return processNode(expressionValueNode, this);
    };
    visitParamNode(paramNode: Param): DateValue | DateFilter | null {
        return null;
    };
    visitCastNode(castNode: Cast): DateValue | DateFilter | null {
        return null;
    };
    visitAggrFuncNode(aggrFuncNode: AggrFunc): DateValue | DateFilter | null {
        return null;
    };
    visitValueNode(valueNode: Value): DateValue | DateFilter | null {
        return null;
    };
    visitExprListNode(exprListNode: ExprList): DateValue | DateFilter | null {
        return null;
    };
};

function processNode(node: ASTNode, visitor: NodeVisitor<DateValue | DateFilter | null>): DateValue | DateFilter | null {
    // console.log("Processing Node: " + JSON.stringify(node, null, 4));
    switch (node.type) {
        case 'column_ref': return visitor.visitColumnRefNode(node as ColumnRefItem);
        case 'param': return visitor.visitParamNode(node as Param);
        case 'function': return visitor.visitFunctionNode(node as Function);
        case 'cast': return visitor.visitCastNode(node as Cast);
        case 'aggr_func': return visitor.visitAggrFuncNode(node as AggrFunc);
        case 'value': return visitor.visitValueNode(node as Value);
        case 'binary_expr': return visitor.visitBinaryNode(node as Binary);
        case 'interval': return visitor.visitIntervalNode(node as Interval);
        case 'expression_value': return visitor.visitExpressionValueNode(node as ExpressionValue);
        case 'expr_list': return visitor.visitExprListNode(node as ExprList);
        case 'extract': return visitor.visitExtractNode(node as extract_func);
        case 'date_value': return visitor.visitDateValueNode(node as DateValue);
        default:
            console.warn(`Unhandled node type: ${node.type}`);
            return null;
    };
};

// This test only cares about 4 basic cases of date filtering:
// 1. Current period
// 2. Last N periods
// 3. Next Period
// 4. Previous Period

// Note that each database(Postgres, MySQL, Snowflake, RedShift, BigQuery) has its own syntax for date functions.

// Assume everything is redshift for now
function getDateFiltersFromSQLQuery({ sqlQuery, database, }: { sqlQuery: string, database: 'postgresql' | 'mysql' | 'snowflake' | 'redshift' | 'bigquery', }): DateFilter[] {
    var res: DateFilter[] = [];
    var collector: any;

    switch (database) {
        case 'redshift':
            collector = new RedShiftASTCollector();
            break;
        case 'snowflake':
            collector = new SnowflakeASTCollector();
            break;
        case 'bigquery':
            collector = new BigQueryASTCollector();
            break;
        case 'mysql':
            collector = new MySQLASTCollector();
            break;
        case 'postgresql':
            collector = new PostgreSQLASTCollector();
            break;
    };

    const parser = new Parser();
    const astObj = parser.astify(sqlQuery, { database });
    const ast = Array.isArray(astObj) ? astObj[0] : astObj;

    const whereCondition = (ast as Select).where;
    if (!whereCondition) { return []; } // Base case

    console.log("Where AST structure:")
    console.log(JSON.stringify(whereCondition, null, 4));
    processNode(whereCondition as unknown as ASTNode, collector);

    return collector.res;
};


////////////////////////////////////
// // Current period Redshift
// Ok
// const ex1CurrentPeriodRedshift = "SELECT * FROM transactions WHERE EXTRACT(MONTH FROM transaction_date) = EXTRACT(MONTH FROM CURRENT_DATE) AND EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE);"
// const res1CurrentPeriodRedshift = getDateFiltersFromSQLQuery({ sqlQuery: ex1CurrentPeriodRedshift, database: "redshift" });
// console.log("\n\nres:");
// console.log(res1CurrentPeriodRedshift);

// // Iffy
// const ex2CurrentPeriodRedshift = "SELECT * FROM transactions WHERE transaction_date >= DATE_TRUNC('month', CURRENT_DATE) AND transaction_date < (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month')";
// const res2CurrentPeriodRedshift = getDateFiltersFromSQLQuery({ sqlQuery: ex2CurrentPeriodRedshift, database: "redshift" });
// console.log("\n\nres:");
// console.log(res2CurrentPeriodRedshift);

// // Iffy
// const ex3CurrentPeriodRedshift = "SELECT * FROM transactions WHERE transaction_date >= DATE_TRUNC('month', CURRENT_DATE) AND transaction_date < (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' + INTERVAL '1 month' - INTERVAL '1 month')";
// const res3CurrentPeriodRedshift = getDateFiltersFromSQLQuery({ sqlQuery: ex3CurrentPeriodRedshift, database: "redshift" });
// console.log("\n\nres:");
// console.log(res3CurrentPeriodRedshift);

// // Last period
// Ok
// const ex1LastPeriodRedshift = "SELECT * FROM transactions WHERE transaction_date >= DATEADD(day, -90, GETDATE());";
// const res1LastPeriodRedshift = getDateFiltersFromSQLQuery({ sqlQuery: ex1LastPeriodRedshift, database: "redshift" });
// console.log("\n\nres:");
// console.log(res1LastPeriodRedshift);

// // Ok
// const ex2LastPeriodRedshift = "SELECT * FROM transactions WHERE transaction_date >= DATEADD(day, -90, CURRENT_DATE);";
// const res2LastPeriodRedshift = getDateFiltersFromSQLQuery({ sqlQuery: ex2LastPeriodRedshift, database: "redshift" });
// console.log("\n\nres:");
// console.log(res2LastPeriodRedshift);

// // Last N periods
// // Ok
// const ex1LastNPeriodRedshift = "SELECT * FROM transactions WHERE transaction_date >= DATEADD(day, -90, GETDATE());";
// const res1LastNPeriodRedshift = getDateFiltersFromSQLQuery({ sqlQuery: ex1LastNPeriodRedshift, database: "redshift" });
// console.log("\n\nres:");
// console.log(res1LastNPeriodRedshift);

// // Not Ok. Empty res
// const ex2LastNPeriodRedshift = "SELECT * FROM transactions WHERE transaction_date >= DATEADD(day, -90, CURRENT_DATE);";
// const res2LastNPeriodRedshift = getDateFiltersFromSQLQuery({ sqlQuery: ex2LastNPeriodRedshift, database: "redshift" });

// // Previous period
// // Iffy
// const ex1PreviousPeriodRedshift = "SELECT * FROM transactions WHERE transaction_date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month' AND transaction_date < DATE_TRUNC('month', CURRENT_DATE);";
// const res1PreviousPeriodRedshift = getDateFiltersFromSQLQuery({ sqlQuery: ex1PreviousPeriodRedshift, database: "redshift" });
// console.log("\n\nres:");
// console.log(res1PreviousPeriodRedshift);

// const ex2PreviousPeriodRedshift = "SELECT * FROM transactions WHERE transaction_date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month';";
// const res2PreviousPeriodRedshift = getDateFiltersFromSQLQuery({ sqlQuery: ex2PreviousPeriodRedshift, database: "redshift" });
// console.log("\n\nres:");
// console.log(res2PreviousPeriodRedshift);

/////////////////////////////////////////

// // Current Period Snowflake
// // Ok
// const ex1CurrentPeriodSnowflake = "SELECT * FROM transactions WHERE YEAR(transaction_date) = YEAR(CURRENT_DATE()) AND MONTH(transaction_date) = MONTH(CURRENT_DATE());";
// const res1CurrentPeriodSnowflake = getDateFiltersFromSQLQuery({ sqlQuery: ex1CurrentPeriodSnowflake, database: "snowflake" });
// console.log("\n\nres:");
// console.log(res1CurrentPeriodSnowflake);

// // Ok
// const ex2CurrentPeriodSnowflake = "SELECT * FROM transactions WHERE EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE())";
// const res2CurrentPeriodSnowflake = getDateFiltersFromSQLQuery({ sqlQuery: ex2CurrentPeriodSnowflake, database: "snowflake" });
// console.log("\n\nres:");
// console.log(res2CurrentPeriodSnowflake);

// // Ok
// const ex3CurrentPeriodSnowflake = "SELECT * FROM transactions WHERE YEAR(transaction_date) = YEAR(CURRENT_DATE()) AND MONTH(transaction_date) = MONTH(CURRENT_DATE());";
// const res3CurrentPeriodSnowflake = getDateFiltersFromSQLQuery({ sqlQuery: ex3CurrentPeriodSnowflake, database: "snowflake" });
// console.log("\n\nres:");
// console.log(res3CurrentPeriodSnowflake);

// // Ok
// const ex4CurrentPeriodSnowflake = "SELECT * FROM transactions WHERE EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE()) AND EXTRACT(MONTH FROM transaction_date) = EXTRACT(MONTH FROM CURRENT_DATE());";
// const res4CurrentPeriodSnowflake = getDateFiltersFromSQLQuery({ sqlQuery: ex4CurrentPeriodSnowflake, database: "snowflake" });
// console.log("\n\nres:");
// console.log(res4CurrentPeriodSnowflake);

// // Last Period Snowflake
// // Ok
// const ex1LastPeriodSnowflake = "SELECT * FROM transactions WHERE transaction_date >= CURRENT_DATE() - INTERVAL '1 YEAR'";
// const res1LastPeriodSnowflake = getDateFiltersFromSQLQuery({ sqlQuery: ex1LastPeriodSnowflake, database: "snowflake" });
// console.log("\n\nres:");
// console.log(res1LastPeriodSnowflake);

// // Ok
// const ex2LastPeriodSnowflake = "SELECT * FROM transactions WHERE transaction_date >= DATEADD(YEAR, -1, CURRENT_DATE())";
// const res2LastPeriodSnowflake = getDateFiltersFromSQLQuery({ sqlQuery: ex2LastPeriodSnowflake, database: "snowflake" });
// console.log("\n\nres:");
// console.log(res2LastPeriodSnowflake);



// // Last N Periods Snowflake
// // Ok
// const ex1LastNPeriodsSnowflake = "SELECT * FROM transactions WHERE transaction_date >= CURRENT_DATE() - INTERVAL '90 DAY';";
// const res1LastNPeriodsSnowflake = getDateFiltersFromSQLQuery({ sqlQuery: ex1LastNPeriodsSnowflake, database: "snowflake" });
// console.log("\n\nres:");
// console.log(res1LastNPeriodsSnowflake);

// // Ok
// const ex2LastNPeriodsSnowflake = "SELECT * FROM transactions WHERE transaction_date >= CURRENT_DATE() - INTERVAL '6 MONTH'";
// const res2LastNPeriodsSnowflake = getDateFiltersFromSQLQuery({ sqlQuery: ex2LastNPeriodsSnowflake, database: "snowflake" });
// console.log("\n\nres:");
// console.log(res2LastNPeriodsSnowflake);

// // Ok
// const ex3LastNPeriodsSnowflake = "SELECT * FROM transactions WHERE transaction_date >= DATEADD(DAY, -90, CURRENT_DATE());";
// const res3LastNPeriodsSnowflake = getDateFiltersFromSQLQuery({ sqlQuery: ex3LastNPeriodsSnowflake, database: "snowflake" });
// console.log("\n\nres:");
// console.log(res3LastNPeriodsSnowflake);



// Previous Period Snowflake
// Iffy - extra current
// const ex1PreviousPeriodSnowflake = "SELECT * FROM transactions WHERE transaction_date >= DATE_TRUNC('MONTH', CURRENT_DATE() - INTERVAL '1 MONTH') AND transaction_date < DATE_TRUNC('MONTH', CURRENT_DATE());";
// const res1PreviousPeriodSnowflake = getDateFiltersFromSQLQuery({ sqlQuery: ex1PreviousPeriodSnowflake, database: "snowflake" });
// console.log("\n\nres:");
// console.log(res1PreviousPeriodSnowflake);


// // Iffy - extra current
// const ex2PreviousPeriodSnowflake = "SELECT * FROM transactions WHERE transaction_date >= DATEADD(month, -1, DATE_TRUNC('MONTH', CURRENT_DATE())) AND transaction_date < DATE_TRUNC('MONTH', CURRENT_DATE());";
// const res2PreviousPeriodSnowflake = getDateFiltersFromSQLQuery({ sqlQuery: ex2PreviousPeriodSnowflake, database: "snowflake" });
// console.log("\n\nres:");
// console.log(res2PreviousPeriodSnowflake);



// const ex3PreviousPeriodSnowflake = "SELECT * FROM transactions WHERE transaction_date BETWEEN DATE_TRUNC('MONTH', CURRENT_DATE() - INTERVAL '1 MONTH') AND LAST_DAY(DATE_TRUNC('MONTH', CURRENT_DATE() - INTERVAL '1 MONTH'));";
// const res3PreviousPeriodSnowflake = getDateFiltersFromSQLQuery({ sqlQuery: ex3PreviousPeriodSnowflake, database: "snowflake" });
// console.log("\n\nres:");
// console.log(res3PreviousPeriodSnowflake);






////////////////////////////////////////////////////////////////
// // Current Period BigQuery
// // Ok
// const ex1CurrentPeriodBigQuery = "SELECT * FROM transactions WHERE EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE()) AND EXTRACT(MONTH FROM transaction_date) = EXTRACT(MONTH FROM CURRENT_DATE())";
// const res1CurrentPeriodBigQuery = getDateFiltersFromSQLQuery({ sqlQuery: ex1CurrentPeriodBigQuery, database: "bigquery" });
// console.log("\n\nres:");
// console.log(res1CurrentPeriodBigQuery);

// // Last Period
// // Ok (started just now)
// const ex2CurrentPeriodBigQuery = "SELECT * FROM transactions WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)";
// const res2CurrentPeriodBigQuery = getDateFiltersFromSQLQuery({ sqlQuery: ex2CurrentPeriodBigQuery, database: "bigquery" });
// console.log("\n\nres:");
// console.log(res2CurrentPeriodBigQuery);

// // Last N Periods
// // Ok
// const ex1LastNPeriodsBigQuery = "SELECT * FROM transactions WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)";
// const res1LastNPeriodsBigQuery = getDateFiltersFromSQLQuery({ sqlQuery: ex1LastNPeriodsBigQuery, database: "bigquery" });
// console.log("\n\nres:");
// console.log(res1LastNPeriodsBigQuery);

// // Ok
// const ex2LastNPeriodsBigQuery = "SELECT * FROM transactions WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)";
// const res2LastNPeriodsBigQuery = getDateFiltersFromSQLQuery({ sqlQuery: ex2LastNPeriodsBigQuery, database: "bigquery" });
// console.log("\n\nres:");
// console.log(res2LastNPeriodsBigQuery);

// // Previous Period
// // Ok
// const ex1PreviousPeriodBigQuery = "SELECT * FROM transactions WHERE PARSE_TIMESTAMP(\"%Y-%M-%D\", transaction_date) = PARSE_TIMESTAMP(\"%Y-%M-%D\", CURRENT_DATE())";
// const res1PreviousPeriodBigQuery = getDateFiltersFromSQLQuery({ sqlQuery: ex1PreviousPeriodBigQuery, database: "bigquery" });
// console.log("\n\nres:");
// console.log(res1PreviousPeriodBigQuery);


// // "Interval MONTH" -> "MONTH"
// // NOT ok -> numberOfPeriods: NaN
// const ex2PreviousPeriodBigQuery = "SELECT * FROM transactions WHERE TIMESTAMP_TRUNC(dateField, MONTH) = TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 30 DAY), MONTH)";
// const res2PreviousPeriodBigQuery = getDateFiltersFromSQLQuery({ sqlQuery: ex2PreviousPeriodBigQuery, database: "bigquery" });
// console.log("\n\nres:");
// console.log(res2PreviousPeriodBigQuery);



// // Current Period MySQL
// // Ok
// const ex1CurrentPeriodMySQL = "SELECT * FROM transactions WHERE YEAR(transaction_date) = YEAR(CURRENT_DATE()) AND MONTH(transaction_date) = MONTH(CURRENT_DATE());";
// const res1CurrentPeriodMySQL = getDateFiltersFromSQLQuery({ sqlQuery: ex1CurrentPeriodMySQL, database: "mysql" });
// console.log("\n\nres:");
// console.log(res1CurrentPeriodMySQL);


// // Ok
// const ex2CurrentPeriodMySQL = "SELECT * FROM transactions WHERE YEAR(transaction_date) = YEAR(CURDATE());";
// const res2CurrentPeriodMySQL = getDateFiltersFromSQLQuery({ sqlQuery: ex2CurrentPeriodMySQL, database: "mysql" });
// console.log("\n\nres:");
// console.log(res2CurrentPeriodMySQL);

// // Last Period
// // Ok
// const ex1LastPeriodMySQL = "SELECT * FROM transactions WHERE transaction_date >= CURDATE() - INTERVAL 1 MONTH;";
// const res1LastPeriodMySQL = getDateFiltersFromSQLQuery({ sqlQuery: ex1LastPeriodMySQL, database: "mysql" });
// console.log("\n\nres:");
// console.log(res1LastPeriodMySQL);

// // Ok
// const ex2LastPeriodMySQL = "SELECT * FROM transactions WHERE transaction_date >= DATE_SUB(CURDATE(), INTERVAL 1 MONTH)";
// const res2LastPeriodMySQL = getDateFiltersFromSQLQuery({ sqlQuery: ex2LastPeriodMySQL, database: "mysql" });
// console.log("\n\nres:");
// console.log(res2LastPeriodMySQL);

// // Ok
// const ex3LastPeriodMySQL = "SELECT * FROM transactions WHERE transaction_date >= DATE_SUB(NOW(), INTERVAL 1 MONTH);";
// const res3LastPeriodMySQL = getDateFiltersFromSQLQuery({ sqlQuery: ex3LastPeriodMySQL, database: "mysql" });
// console.log("\n\nres:");
// console.log(res3LastPeriodMySQL);


// // Last N Periods
// // Ok
// const ex1LastNPeriodsMySQL = "SELECT * FROM transactions WHERE transaction_date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY);";
// const res1LastNPeriodsMySQL = getDateFiltersFromSQLQuery({ sqlQuery: ex1LastNPeriodsMySQL, database: "mysql" });
// console.log("\n\nres:");
// console.log(res1LastNPeriodsMySQL);

// // Ok
// const ex2LastNPeriodsMySQL = "SELECT * FROM transactions WHERE transaction_date >= CURDATE() - INTERVAL 90 DAY;";
// const res2LastNPeriodsMySQL = getDateFiltersFromSQLQuery({ sqlQuery: ex2LastNPeriodsMySQL, database: "mysql" });
// console.log("\n\nres:");
// console.log(res2LastNPeriodsMySQL);



// // Current Period PostgreSQL
// // Ok
// const ex1CurrentPeriodPostgreSQL = "SELECT * FROM transactions WHERE DATE_TRUNC('month', transaction_date) = DATE_TRUNC('month', CURRENT_DATE);";
// const res1CurrentPeriodPostgreSQL = getDateFiltersFromSQLQuery({ sqlQuery: ex1CurrentPeriodPostgreSQL, database: "postgresql" });
// console.log("\n\nres:");
// console.log(res1CurrentPeriodPostgreSQL);

// // Ok
// const ex2CurrentPeriodPostgreSQL = "SELECT * FROM transactions WHERE DATE_TRUNC('month', transaction_date) = DATE_TRUNC('month', CURRENT_DATE);";
// const res2CurrentPeriodPostgreSQL = getDateFiltersFromSQLQuery({ sqlQuery: ex2CurrentPeriodPostgreSQL, database: "postgresql" });
// console.log("\n\nres:");
// console.log(res2CurrentPeriodPostgreSQL);

// // Iffy: extra last filter
// const ex3CurrentPeriodPostgreSQL = "SELECT * FROM transactions WHERE created_at >= date_trunc('month', CURRENT_DATE) AND created_at < (date_trunc('month', CURRENT_DATE) + INTERVAL '1 month');";
// const res3CurrentPeriodPostgreSQL = getDateFiltersFromSQLQuery({ sqlQuery: ex3CurrentPeriodPostgreSQL, database: "postgresql" });
// console.log("\n\nres:");
// console.log(res3CurrentPeriodPostgreSQL);

// // Last Period PostgreSQL
// // Ok
// const ex1LastPeriodPostgreSQL = "SELECT * FROM transactions WHERE transaction_date >= CURRENT_DATE - INTERVAL '1 month';";
// const res1LastPeriodPostgreSQL = getDateFiltersFromSQLQuery({ sqlQuery: ex1LastPeriodPostgreSQL, database: "postgresql" });
// console.log("\n\nres:");
// console.log(res1LastPeriodPostgreSQL);


// // Last N Periods PostgreSQL
// // Ok
// const ex1LastNPeriodsPostgreSQL = "SELECT * FROM transactions WHERE transaction_date >= CURRENT_DATE - INTERVAL '30 months';";
// const res1LastNPeriodsPostgreSQL = getDateFiltersFromSQLQuery({ sqlQuery: ex1LastNPeriodsPostgreSQL, database: "postgresql" });
// console.log("\n\nres:");
// console.log(res1LastNPeriodsPostgreSQL);

// // Previous Period PostgreSQL
// // Ok
// const ex1PreviousPeriodPostgreSQL = "SELECT * FROM transactions WHERE DATE_TRUNC('month', transaction_date) = DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month');";
// const res1PreviousPeriodPostgreSQL = getDateFiltersFromSQLQuery({ sqlQuery: ex1PreviousPeriodPostgreSQL, database: "postgresql" });
// console.log("\n\nres:");
// console.log(res1PreviousPeriodPostgreSQL);

// // Ok
// const ex2PreviousPeriodPostgreSQL = "SELECT * FROM transactions WHERE DATE_TRUNC('quarter', transaction_date) = DATE_TRUNC('quarter', CURRENT_DATE - INTERVAL '1 quarter');";
// const res2PreviousPeriodPostgreSQL = getDateFiltersFromSQLQuery({ sqlQuery: ex2PreviousPeriodPostgreSQL, database: "postgresql" });
// console.log("\n\nres:");
// console.log(res2PreviousPeriodPostgreSQL);

// // Iffy
// const ex3PreviousPeriodPostgreSQL = "SELECT * FROM transactions WHERE EXTRACT(MONTH FROM transaction_date) = EXTRACT(MONTH FROM CURRENT_DATE - INTERVAL '1 month') AND EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE - INTERVAL '1 month');";
// const res3PreviousPeriodPostgreSQL = getDateFiltersFromSQLQuery({ sqlQuery: ex3PreviousPeriodPostgreSQL, database: "postgresql" });
// console.log("\n\nres:");
// console.log(res3PreviousPeriodPostgreSQL);