/*
 * Copyright (c) 2024 vitasystems GmbH.
 *
 * This file is part of project EHRbase
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ehrbase.openehr.aqlengine.sql;

import static org.ehrbase.jooq.pg.Tables.COMP_DATA;
import static org.ehrbase.jooq.pg.Tables.COMP_VERSION;
import static org.ehrbase.jooq.pg.Tables.EHR_FOLDER_DATA;
import static org.ehrbase.jooq.pg.Tables.EHR_FOLDER_VERSION;
import static org.ehrbase.jooq.pg.Tables.EHR_STATUS_DATA;
import static org.ehrbase.jooq.pg.Tables.EHR_STATUS_VERSION;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.tuple.Pair;
import org.ehrbase.api.knowledge.KnowledgeCacheService;
import org.ehrbase.jooq.pg.util.AdditionalSQLFunctions;
import org.ehrbase.openehr.aqlengine.asl.model.field.AslAggregatingField;
import org.ehrbase.openehr.aqlengine.asl.model.field.AslColumnField;
import org.ehrbase.openehr.aqlengine.asl.model.field.AslComplexExtractedColumnField;
import org.ehrbase.openehr.aqlengine.asl.model.field.AslConstantField;
import org.ehrbase.openehr.aqlengine.asl.model.field.AslField;
import org.ehrbase.openehr.aqlengine.asl.model.field.AslFolderItemIdValuesColumnField;
import org.ehrbase.openehr.aqlengine.asl.model.field.AslSubqueryField;
import org.ehrbase.openehr.aqlengine.asl.model.field.AslVirtualField;
import org.ehrbase.openehr.aqlengine.asl.model.join.AslJoin;
import org.ehrbase.openehr.aqlengine.asl.model.query.AslEncapsulatingQuery;
import org.ehrbase.openehr.aqlengine.asl.model.query.AslFilteringQuery;
import org.ehrbase.openehr.aqlengine.asl.model.query.AslPathDataQuery;
import org.ehrbase.openehr.aqlengine.asl.model.query.AslQuery;
import org.ehrbase.openehr.aqlengine.asl.model.query.AslRmObjectDataQuery;
import org.ehrbase.openehr.aqlengine.asl.model.query.AslRootQuery;
import org.ehrbase.openehr.aqlengine.asl.model.query.AslStructureQuery;
import org.ehrbase.openehr.aqlengine.asl.model.query.AslStructureQuery.AslSourceRelation;
import org.ehrbase.openehr.aqlengine.sql.postprocessor.AqlSqlQueryPostProcessor;
import org.ehrbase.openehr.dbformat.RmAttributeAlias;
import org.ehrbase.openehr.dbformat.RmTypeAlias;
import org.ehrbase.openehr.dbformat.jooq.prototypes.ObjectDataTablePrototype;
import org.ehrbase.openehr.sdk.aql.dto.path.AqlObjectPath.PathNode;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.JSONObjectAggNullStep;
import org.jooq.Operator;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Result;
import org.jooq.SelectConditionStep;
import org.jooq.SelectField;
import org.jooq.SelectFieldOrAsterisk;
import org.jooq.SelectHavingStep;
import org.jooq.SelectJoinStep;
import org.jooq.SelectQuery;
import org.jooq.SelectSelectStep;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.TableLike;
import org.jooq.impl.DSL;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Builds an SQL query from an ASL query
 */
@Component
public class AqlSqlQueryBuilder {

    private final DSLContext context;
    private final KnowledgeCacheService knowledgeCache;
    private final Optional<AqlSqlQueryPostProcessor> queryPostProcessor;

    @Value("${ehrbase.aql.pg-llj-workaround}")
    private boolean pgLljWorkaround = false;

    public AqlSqlQueryBuilder(
            DSLContext context,
            KnowledgeCacheService knowledgeCache,
            Optional<AqlSqlQueryPostProcessor> queryPostProcessor) {
        this.context = context;
        this.knowledgeCache = knowledgeCache;
        this.queryPostProcessor = queryPostProcessor;
    }

    public static String subqueryAlias(AslQuery aslQuery) {
        return aslQuery.getAlias() + "sq";
    }

    public static String versionSubqueryAlias(AslQuery aslQuery) {
        return aslQuery.getAlias() + "_version_sq";
    }

    /**
     * Resolves the data and version jooq tables (sql tables or sub-queries) the <code>AslQuery</code> is based on
     */
    static class AslQueryTables {

        private Map<AslQuery, Table<?>> dataTables = new HashMap<>();
        private Map<AslQuery, Table<?>> versionTables = new HashMap<>();

        private AslQueryTables() {}

        Table<?> getDataTable(AslQuery q) {
            return dataTables.get(q);
        }

        Table<?> getVersionTable(AslQuery q) {
            return versionTables.get(q);
        }

        public void put(AslQuery q, Table<?> dataTable, Table<?> versionTable) {
            dataTables.put(q, dataTable);
            versionTables.put(q, versionTable);
        }

        public void remove(AslStructureQuery aq) {
            dataTables.remove(aq);
            versionTables.remove(aq);
        }
    }

    public SelectQuery<Record> buildSqlQuery(AslRootQuery aslRootQuery) {

        AslQueryTables aslQueryToTable = new AslQueryTables();
        SelectJoinStep<Record> encapsulatingQuery =
                buildEncapsulatingQuery(aslRootQuery, context::select, aslQueryToTable);

        SelectQuery<Record> query = encapsulatingQuery.getQuery();

        // LIMIT
        if (aslRootQuery.getLimit() != null) {
            query.addLimit(aslRootQuery.getOffset() == null ? 0L : aslRootQuery.getOffset(), aslRootQuery.getLimit());
        }

        queryPostProcessor.ifPresent(p -> p.afterBuildSqlQuery(aslRootQuery, query));

        return query;
    }

    public Result<Record> explain(boolean analyze, SelectQuery<Record> selectQuery) {
        if (analyze) {
            return context.fetch("EXPLAIN (SUMMARY, COSTS, VERBOSE, FORMAT JSON, ANALYZE, TIMING) {0}", selectQuery);
        } else {
            return context.fetch("EXPLAIN (SUMMARY, COSTS, VERBOSE, FORMAT JSON) {0}", selectQuery);
        }
    }

    @Nonnull
    private SelectJoinStep<Record> buildEncapsulatingQuery(
            AslEncapsulatingQuery aq, Supplier<SelectSelectStep<Record>> creator, AslQueryTables aslQueryToTable) {
        Iterator<Pair<AslQuery, AslJoin>> childIt = aq.getChildren().iterator();

        // from

        AslQuery aslRoot = childIt.next().getLeft();
        Table<?> root = buildQuery(aslRoot, null, aslQueryToTable);
        aslQueryToTable.put(aslRoot, root, root);
        SelectJoinStep<Record> from = creator.get().from(root);

        while (childIt.hasNext()) {
            Pair<AslQuery, AslJoin> nextChild = childIt.next();
            AslQuery childQuery = nextChild.getLeft();
            AslJoin join = nextChild.getRight();
            AslQuery target = join.getLeft();
            Table<?> toJoin = buildQuery(childQuery, target, aslQueryToTable);

            if (pgLljWorkaround) {
                EncapsulatingQueryUtils.applyPgLljWorkaround(childQuery, join, toJoin);
            }

            aslQueryToTable.put(childQuery, toJoin, toJoin);
            from.join(toJoin, join.getJoinType()).on(ConditionUtils.buildJoinCondition(join, aslQueryToTable));
        }

        SelectQuery<Record> query = from.getQuery();
        // select
        for (AslField field : aq.getSelect()) {
            SelectField<?> sqlField = EncapsulatingQueryUtils.selectField(field, aslQueryToTable);
            query.addSelect(sqlField);
        }
        // where
        query.addConditions(
                Operator.AND,
                Stream.concat(
                                Optional.of(aq).map(AslEncapsulatingQuery::getCondition).stream(),
                                aq.getStructureConditions().stream())
                        .map(c -> ConditionUtils.buildCondition(c, aslQueryToTable, true))
                        .toList());

        if (aq instanceof AslRootQuery rq) {
            rq.getGroupByFields().stream()
                    .flatMap(gb -> EncapsulatingQueryUtils.groupByFields(gb, aslQueryToTable))
                    .forEach(query::addGroupBy);

            // if the magnitude is needed for ORDER BY, it is added to the GROUP BY
            rq.getGroupByDvOrderedMagnitudeFields().stream()
                    .map(f -> AdditionalSQLFunctions.jsonb_dv_ordered_magnitude((Field<JSONB>)
                            FieldUtils.field(aslQueryToTable.getDataTable(f.getInternalProvider()), f, true)))
                    .forEach(query::addGroupBy);

            rq.getOrderByFields().stream()
                    .flatMap(ob -> EncapsulatingQueryUtils.orderFields(ob, aslQueryToTable, knowledgeCache))
                    .forEach(query::addOrderBy);
        }

        return from;
    }

    private Table<?> buildQuery(AslQuery aslQuery, AslQuery target, AslQueryTables aslQueryToTable) {
        return switch (aslQuery) {
            case AslStructureQuery aq -> buildStructureQuery(aq, aslQueryToTable)
                    .asTable(aq.getAlias());
            case AslEncapsulatingQuery aq -> buildEncapsulatingQuery(aq, DSL::select, aslQueryToTable)
                    .asTable(aq.getAlias());
            case AslRmObjectDataQuery aq -> DSL.lateral(
                    buildDataSubquery(aq, aslQueryToTable).asTable(aq.getAlias()));
            case AslFilteringQuery aq -> DSL.lateral(buildFilteringQuery(aq, aslQueryToTable.getDataTable(target))
                    .asTable(aq.getAlias()));
            case AslPathDataQuery aq -> DSL.lateral(
                    buildPathDataQuery(aq, target, aslQueryToTable).asTable(aq.getAlias()));
        };
    }

    private static AslSourceRelation getTargetType(AslQuery target) {
        if (target instanceof AslStructureQuery sq) {
            return sq.getType();
        } else {
            throw new IllegalArgumentException("target is no StructureQuery: %s".formatted(target));
        }
    }

    /**
     * Has to be wrapped in DSL::lateral.
     * Applies "jsonb_array_elements" function, if last node is multiple valued
     * <p>
     * Structure based:
     * <p>
     * select "cData"."data"->'N' as "pd_0_data"
     * from "ehr"."comp" as "cData"
     * where (
     * "sSE_s_0"."sSE_s_0_ehr_id" = "cData"."ehr_id"
     * and "sSE_s_0"."sSE_s_0_vo_id" = "cData"."vo_id"
     * and "sSE_s_0"."sSE_s_0_entity_idx" = "cData"."entity_idx"
     * )
     * <p>
     * Path data based:
     * <p>
     * select "cData"."data"->'N' as "pd_0_data"
     *
     * @param aslData
     * @param target
     * @return
     */
    private static TableLike<Record> buildPathDataQuery(
            AslPathDataQuery aslData, AslQuery target, AslQueryTables aslQueryToTable) {
        Table<?> targetTable = aslQueryToTable.getDataTable(target);

        AslQuery base = aslData.getBase();

        Table<?> data;
        Function<String, Field<JSONB>> dataFieldProvider;
        if (base instanceof AslStructureQuery baseSq) {
            data = baseSq.getType().getDataTable().as(subqueryAlias(aslData));
            dataFieldProvider = __ -> data.field(ObjectDataTablePrototype.INSTANCE.DATA);
        } else {
            data = targetTable;
            dataFieldProvider = colName -> FieldUtils.aliasedField(data, aslData, colName, JSONB.class);
        }

        SelectSelectStep<Record> select = DSL.select(aslData.getSelect().stream()
                .map(AslColumnField.class::cast)
                .map(f -> pathDataField(aslData, f, dataFieldProvider))
                .toList());

        if (base instanceof AslStructureQuery) {
            // primary key condition
            List<Condition> pkeyCondition = data.getPrimaryKey().getFields().stream()
                    .map(f -> FieldUtils.aliasedField(targetTable, aslData, f).eq((Field) data.field(f)))
                    .toList();

            return select.from(data).where(pkeyCondition);

        } else {
            return select;
        }
    }

    @Nonnull
    private static Field pathDataField(
            AslPathDataQuery aslData, AslColumnField f, Function<String, Field<JSONB>> dataFieldProvider) {
        Field<JSONB> dataField = dataFieldProvider.apply(f.getColumnName());
        Field<JSONB> jsonbField = buildJsonbPathField(aslData.getPathNodes(f), aslData.isMultipleValued(), dataField);
        Field<?> field;
        if (f.getType() == String.class) {
            field = DSL.jsonbGetElementAsText(jsonbField, 0);
        } else {
            field = jsonbField;
        }
        return field.as(f.getName(true));
    }

    private static Field<JSONB> buildJsonbPathField(
            List<PathNode> pathNodes, boolean multipleValued, Field<JSONB> jsonbField) {
        Iterator<String> attributeIt = pathNodes.stream()
                .map(PathNode::getAttribute)
                .map(RmAttributeAlias::getAlias)
                .iterator();

        Field<JSONB> field = jsonbField;

        while (attributeIt.hasNext()) {
            field = DSL.jsonbGetAttribute(field, DSL.inline(attributeIt.next()));
        }

        if (multipleValued) {
            field = AdditionalSQLFunctions.jsonb_array_elements(field);
        }

        return field;
    }

    private static SelectSelectStep<?> buildFilteringQuery(AslFilteringQuery aq, Table<?> target) {
        Stream<Field> fields =
                switch (aq.getSourceField()) {
                    case AslColumnField src -> Stream.of(FieldUtils.field(target, src, true)
                            .as(((AslColumnField) aq.getSelect().getFirst()).getAliasedName()));
                    case AslComplexExtractedColumnField src -> src.getExtractedColumn().getColumns().stream()
                            .map(fieldName -> FieldUtils.field(target, src, fieldName, true)
                                    .as(src.aliasedName(fieldName)));
                    case AslConstantField<?> cf -> Stream.of(DSL.inline(cf.getValue(), cf.getType()));
                    case AslAggregatingField __ -> throw new IllegalArgumentException(
                            "Filtering queries cannot be based on AslAggregatingField");
                    case AslSubqueryField __ -> throw new IllegalArgumentException(
                            "Filtering queries cannot be based on AslSubqueryField");
                    case AslFolderItemIdValuesColumnField __ -> throw new IllegalArgumentException(
                            "Filtering queries cannot be based on AslFolderItemIdValuesColumnField");
                };
        return DSL.select(fields.toArray(Field[]::new));
    }

    @Nonnull
    private static SelectConditionStep<Record> buildStructureQuery(
            AslStructureQuery aq, AslQueryTables aslQueryToTable) {
        Table<?> dataTable = aq.getType().getDataTable().as(subqueryAlias(aq));
        Table<?> primaryTable = aq.isRequiresVersionTableJoin()
                ? aq.getType().getVersionTable().as(versionSubqueryAlias(aq))
                : dataTable;

        SelectJoinStep<Record> step = structureQueryBase(aq, primaryTable, dataTable, aq.isRequiresVersionTableJoin());

        aslQueryToTable.put(aq, dataTable, primaryTable);

        // add regular and structure conditions
        SelectConditionStep<Record> where = step.where(Stream.concat(
                        Optional.of(aq).map(AslStructureQuery::getCondition).stream(),
                        aq.getStructureConditions().stream())
                .map(c -> ConditionUtils.buildCondition(c, aslQueryToTable, false))
                .toArray(Condition[]::new));

        // data and primary are local to this sub-query and can be removed
        aslQueryToTable.remove(aq);
        return where;
    }

    @Nonnull
    private static SelectJoinStep<Record> structureQueryBase(
            AslStructureQuery aq, Table<?> primaryTable, Table<?> dataTable, boolean hasVersionTable) {

        Map<Class<? extends AslField>, List<AslField>> aslFields =
                aq.getSelect().stream().collect(Collectors.groupingBy(AslField::getClass));

        Stream<Field<?>> columnFields = Optional.of(aslFields.remove(AslColumnField.class))
                .orElseThrow(
                        () -> new IllegalStateException("StructureQueryBaseSelect does not contain any AslColumnField"))
                .stream()
                .map(AslColumnField.class::cast)
                .map(cf -> ((aq.isRequiresVersionTableJoin() && cf.isVersionTableField()) ? primaryTable : dataTable)
                        .field(cf.getColumnName())
                        .as(cf.getAliasedName()));

        final SelectJoinStep<Record> step;

        if (hasVersionTable) {

            final Table<?> joinTable;
            final Condition onCondition;
            final List<Field<?>> selectFields;

            // join version and data table
            switch (aq.getType()) {
                case EHR_STATUS -> {
                    joinTable = dataTable;
                    onCondition = Objects.requireNonNull(primaryTable.field(EHR_STATUS_VERSION.EHR_ID))
                            .eq(dataTable.field(EHR_STATUS_DATA.EHR_ID));
                    selectFields = List.of();
                }
                case COMPOSITION -> {
                    joinTable = dataTable;
                    onCondition = Objects.requireNonNull(primaryTable.field(COMP_VERSION.VO_ID))
                            .eq(dataTable.field(COMP_DATA.VO_ID));
                    selectFields = List.of();
                }
                case FOLDER -> {
                    List<AslFolderItemIdValuesColumnField> folderItemColumns = Optional.ofNullable(
                                    aslFields.remove(AslFolderItemIdValuesColumnField.class))
                            .orElseGet(List::of)
                            .stream()
                            .filter(AslFolderItemIdValuesColumnField.class::isInstance)
                            .map(AslFolderItemIdValuesColumnField.class::cast)
                            .toList();

                    if (!folderItemColumns.isEmpty()) {

                        if (folderItemColumns.size() != 1) {
                            throw new IllegalStateException(
                                    "StructureQueryBase on FOLDER does only support a single AslFolderItemIdValuesColumnField but received %s"
                                            .formatted(folderItemColumns.stream()
                                                    .map(AslVirtualField::getExtractedColumn)));
                        }

                        AslFolderItemIdValuesColumnField column = folderItemColumns.getFirst();

                        Pair<SelectJoinStep<Record>, Field<UUID>> selectJoinStepFieldPair =
                                structureQueryFolderItems(column.getColumnName());

                        joinTable = selectJoinStepFieldPair.getLeft().asTable(dataTable);
                        selectFields = List.of(FieldUtils.complexAliasedField(
                                joinTable, selectJoinStepFieldPair.getRight(), column, column.getColumnName()));
                    } else {
                        joinTable = dataTable;
                        selectFields = List.of();
                    }

                    onCondition = primaryTable
                            .field(EHR_FOLDER_VERSION.EHR_ID)
                            .eq(dataTable.field(EHR_FOLDER_DATA.EHR_ID))
                            .and(primaryTable
                                    .field(EHR_FOLDER_VERSION.EHR_FOLDERS_IDX)
                                    .eq(dataTable.field(EHR_FOLDER_DATA.EHR_FOLDERS_IDX)));
                }
                default -> throw new IllegalArgumentException("%s has no version table".formatted(aq.getType()));
            }

            step = DSL.select(Stream.concat(columnFields, selectFields.stream()).toArray(SelectFieldOrAsterisk[]::new))
                    .from(primaryTable)
                    .join(joinTable)
                    .on(onCondition);
        } else {

            if (!aslFields.isEmpty()) {
                throw new IllegalStateException(
                        "StructureQueryBase without version can only contains AslColumnFields columns %s"
                                .formatted(aslFields.values().stream()
                                        .flatMap(Collection::stream)
                                        .map(AslField::getExtractedColumn)));
            }

            step = DSL.select(columnFields.toArray(SelectFieldOrAsterisk[]::new))
                    .from(primaryTable);
        }
        return step;
    }

    /**
     * Structure sub-query that aggregate all <code>FOLDER.items[].id.value</code> attributes.
     * </p>
     * select
     *   *,
     *   cast(((("items"->'X')->'V')->>0) as uuid) "item_id_value"      -- Folder.items[].id.value
     * from "ehr"."ehr_folder_data"
     *   join jsonb_array_elements(("ehr"."ehr_folder_data"."data"->'i')) as "items"
     *     on (
     *       ((("items"->'X')->'T')->>0) = 'HX'                         -- Folder.items[].id._type = HIER_OBJECT_ID
     *       and (((("items"->'X')->'V')->>0) ~ E'^[[:xdigit:]]{8}-([[:xdigit:]]{4}-){3}[[:xdigit:]]{12}$') -- is UUID
     *     )
     *
     * @return folderItemIdAggregate sub-selection
     */
    @Nonnull
    private static Pair<SelectJoinStep<Record>, Field<UUID>> structureQueryFolderItems(String itemIdValues) {

        Field<JSONB> itemsField = DSL.jsonbGetAttribute(
                EHR_FOLDER_DATA.field(ObjectDataTablePrototype.INSTANCE.DATA),
                DSL.inline(RmAttributeAlias.getAlias("items")));

        Field<JSONB> items =
                AdditionalSQLFunctions.jsonb_array_elements(itemsField).as("items");
        Field<String> itemIdValue = AdditionalSQLFunctions.jsonbAttributePathText(
                items, Stream.of("id", "value").map(RmAttributeAlias::getAlias));
        Field<String> itemIdType = AdditionalSQLFunctions.jsonbAttributePathText(
                items, Stream.of("id", "_type").map(RmAttributeAlias::getAlias));
        Field<UUID> itemIdField = itemIdValue.cast(UUID.class).as(itemIdValues);

        SelectSelectStep<Record> select = DSL.select(
                DSL.asterisk(), // we need all fields at this point
                // cast((("items"->'X')->>'V') as uuid) "item_id_value"
                itemIdField);
        SelectJoinStep<Record> step = select.from(EHR_FOLDER_DATA
                .join(AdditionalSQLFunctions.join_jsonb_array_elements(items))
                // (("items"->'X')->>'T') = 'HX'
                // FIXME not sure if we need this - we could also assume it's an HIER_OBJECT_ID because we check the
                //       uuid any way.
                .on(itemIdType.eq(DSL.inline(RmTypeAlias.getAlias("HIER_OBJECT_ID"))))
                // (((("items"->'X')->'V')->>0) ~ E'^[[:xdigit:]]{8}-([[:xdigit:]]{4}-){3}[[:xdigit:]]{12}$')
                .and(AdditionalSQLFunctions.regexMatches(
                        itemIdValue, "^[[:xdigit:]]{8}-([[:xdigit:]]{4}-){3}[[:xdigit:]]{12}$")));

        return Pair.of(step, itemIdField);
    }

    /**
     * select
     * jsonb_object_agg(
     * ( sub_string(d2."entity_idx" FROM char_length(c2."entity_idx") + 1)
     * ), "data"
     * ) as "data"
     * from "ehr"."comp_one" d2
     * where
     * c2."ehr_id" = "d2"."ehr_id"
     * and c2."VO_ID" = "d2"."VO_ID"
     * and c2."num" <= "d2"."num"
     * and c2."num_cap" >= "d2"."num"
     * group by "d2"."VO_ID"
     */
    static SelectHavingStep<Record1<JSONB>> buildDataSubquery(
            AslRmObjectDataQuery aslData, AslQueryTables aslQueryToTable, Condition... additionalConditions) {
        AslQuery target = aslData.getBaseProvider();
        Table<?> targetTable = aslQueryToTable.getDataTable(target);
        AslSourceRelation type = getTargetType(aslData.getBase());

        Table<?> data = type.getDataTable().as(subqueryAlias(aslData));
        String dataFieldName = ((AslColumnField) aslData.getSelect().getFirst()).getName(true);
        // XXX Data aggregation is not needed for "terminal" structure nodes, e.g. ELEMENT
        Field<JSONB> jsonbField = dataAggregation(
                        data, FieldUtils.aliasedField(targetTable, aslData, COMP_DATA.ENTITY_IDX))
                .as(DSL.name(dataFieldName));

        SelectJoinStep<Record1<JSONB>> from = DSL.select(jsonbField).from(data);

        // primary key condition
        List<Field> pKeyFields = type.getPkeyFields().stream()
                .map((TableField<?, ?> field) -> {
                    Field f = data.field(field);
                    // add EQ to WHERE
                    from.where(
                            FieldUtils.aliasedField(targetTable, aslData, field).eq(f));
                    return f;
                })
                .toList();

        Condition[] conditions = Stream.concat(
                        // TODO can be skipped for roots
                        // TODO can be set to == for leafs (ELEMENT)
                        Stream.of(Objects.requireNonNull(data.field(COMP_DATA.NUM))
                                .between(
                                        FieldUtils.aliasedField(targetTable, aslData, COMP_DATA.NUM),
                                        FieldUtils.aliasedField(targetTable, aslData, COMP_DATA.NUM_CAP))),
                        Arrays.stream(additionalConditions))
                .toArray(Condition[]::new);

        return from.where(conditions).groupBy(pKeyFields);
    }

    /**
     * The aggregated jsonb can be processed by DbToRmFormat::reconstructFromDbFormat
     *
     * @return
     */
    private static JSONObjectAggNullStep<JSONB> dataAggregation(Table<?> dataTable, Field<String> baseEntityIndex) {
        return DSL.jsonbObjectAgg(
                DSL.substring(
                        dataTable.field(COMP_DATA.ENTITY_IDX),
                        DSL.length(baseEntityIndex).plus(DSL.inline(1))),
                dataTable.field(COMP_DATA.DATA));
    }
}
