package alpha;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class Contractor {
    @QuerySqlField(name = "ID_LE")
    public long idLe;

    @QuerySqlField(name = "RESIDENT")
    public Boolean resident;
}
