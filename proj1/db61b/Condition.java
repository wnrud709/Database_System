package db61b;
import static db61b.Utils.error;
import java.util.List;

/**
 * Represents a single 'where' condition in a 'select' command.
 * @author JuKyung Choi
 */
class Condition {

    /**
     * Internally, we represent our relation as a 3-bit value whose bits denote
     * whether the relation allows the left value to be greater than the right
     * (GT), equal to it (EQ), or less than it (LT).
     */
    private static final int GT = 1, EQ = 2, LT = 4, NE = 6, GE = 3, LE = 5;

    /**
     * A Condition representing COL1 RELATION COL2, where COL1 and COL2 are
     * column designators. and RELATION is one of the strings "<", ">", "<=",
     * ">=", "=", or "!=".
     */
    Condition(Column col1, String relation, Column col2) {
        _col1 = col1;
        _col2 = col2;
        switch (relation) {
        case "<":
            _relation = LT;
            break;
        case ">":
            _relation = GT;
            break;
        case "=":
            _relation = EQ;
            break;
        case "<=":
            _relation = LE;
            break;
        case ">=":
            _relation = GE;
            break;
        case "!=":
            _relation = NE;
            break;
        default:
            throw error("%s is not a valid relation", relation);
        }
    }

    /**
     * A Condition representing COL1 RELATION 'VAL2', where COL1 is a column
     * designator, VAL2 is a literal value (without the quotes), and RELATION is
     * one of the strings "<", ">", "<=", ">=", "=", or "!=".
     */
    Condition(Column col1, String relation, String val2) {
        this(col1, relation, new Literal(val2));
    }

    /**
     * Assuming that ROWS are rows from the respective tables from which my
     * columns are selected, returns the result of performing the test I denote.
     */
    boolean test() {
        switch (_relation) {
        case LT:
            return _col1.value().compareTo(_col2.value()) < 0;
        case GT:
            return _col1.value().compareTo(_col2.value()) > 0;
        case LE:
            return _col1.value().compareTo(_col2.value()) <= 0;
        case GE:
            return _col1.value().compareTo(_col2.value()) >= 0;
        case EQ:
            return _col1.value().compareTo(_col2.value()) == 0;
        default:
            return _col1.value().compareTo(_col2.value()) != 0;
        }
    }

    /** Return true iff all CONDITIONS are satified. */
    static boolean test(List<Condition> conditions) {
        for (int i = 0; i < conditions.size(); i++) {
            if (!conditions.get(i).test()) {
                return false;
            }
        }
        return true;
    }

    /** _COL1 is the column being compared. */
    private Column _col1;
    /** _COL2 is the column _COL1 is being compared to. */
    private Column _col2;
    /** _RELATION is the relation. */
    private int _relation;
}
