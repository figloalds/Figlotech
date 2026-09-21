# FieldAttribute.Type portability audit

Audited 2026-09-21 against the current Figlotech and sibling ErpSoftLeader working trees. This is a source audit and refactor proposal; no runtime code or database schema was changed.

## Scope and findings

ErpSoftLeader contains **128 explicit Field(Type = ...) declarations in 73 C# files**, with **11 distinct SQL type strings**. Of these, 126 declarations are in ErpSoftLeader.Business and two are in ErpSoftLeader.Core. Counts describe source declarations, not deployed database columns or inherited copies. Build artifacts were excluded. Searches also covered multiline attributes, FieldAttribute spelling, positional string constructors, and nonliteral Type assignments; the explicit overrides found all use the literals listed below.

## Mapping table

Enum names below are proposed members of FieldType. DecimalPrecision means total digits; DecimalScale means fractional digits. Keeping the text/binary size tiers preserves existing MySQL DDL.

| Current MySQL override | Occurrences | Proposed enum and parameters | PostgreSQL DDL | SQLite DDL / behavior |
| --- | ---: | --- | --- | --- |
| TEXT | 61 | Text | TEXT | TEXT |
| MEDIUMTEXT | 13 | MediumText | TEXT | TEXT |
| LONGTEXT | 3 | LongText | TEXT | TEXT |
| BLOB | 31 | Blob | BYTEA | BLOB |
| MEDIUMBLOB | 6 | MediumBlob | BYTEA | BLOB |
| JSON | 1 | Json | JSONB (recommended); JSON if lexical preservation is required | TEXT plus CHECK (column IS NULL OR json_valid(column)) |
| INT | 1 | Int32 | INTEGER (INT4 for current schema comparison) | INTEGER |
| DECIMAL(18,2) | 4 | Decimal, DecimalPrecision = 18, DecimalScale = 2 | NUMERIC(18,2) | NUMERIC affinity only; no native fixed-decimal equivalent |
| DECIMAL(18,3) | 4 | Decimal, DecimalPrecision = 18, DecimalScale = 3 | NUMERIC(18,3) | NUMERIC affinity only; no native fixed-decimal equivalent |
| DECIMAL(18,6) | 3 | Decimal, DecimalPrecision = 18, DecimalScale = 6 | NUMERIC(18,6) | NUMERIC affinity only; no native fixed-decimal equivalent |
| DECIMAL(26,12) | 1 | Decimal, DecimalPrecision = 26, DecimalScale = 12 | NUMERIC(26,12) | NUMERIC affinity only; no native fixed-decimal equivalent |

PostgreSQL provides text, bytea, integer and exact numeric types: [PostgreSQL type reference](https://www.postgresql.org/docs/current/datatype.html). SQLite mappings follow its [type affinity rules](https://www.sqlite.org/datatype3.html).

### Semantic differences

- **Capacity:** MySQL TEXT/BLOB, MEDIUMTEXT/MEDIUMBLOB and LONGTEXT have byte ceilings of 65,535, 16,777,215 and 4,294,967,295 respectively. PostgreSQL and SQLite do not have these tiers. These mappings preserve the kind of data, not identical maximum capacity or constraints. [MySQL storage requirements](https://dev.mysql.com/doc/refman/8.4/en/storage-requirements.html)
- **Large values:** PostgreSQL's field limit is 1 GB. SQLite's default maximum string/BLOB length is 1,000,000,000 bytes and can vary by build/runtime settings. Mapping LONGTEXT to TEXT does not preserve its entire theoretical MySQL capacity. [PostgreSQL limits](https://www.postgresql.org/docs/current/limits.html), [SQLite limits](https://www.sqlite.org/limits.html)
- **Decimals:** SQLite accepts DECIMAL(p,s), but ignores p/s and applies NUMERIC affinity, which can store floating-point values. Do not present NUMERIC as an exact equivalent. An exact storage policy requires parameter/read conversion: scaled INTEGER can cover all 18-digit declarations, while the full DECIMAL(26,12) range exceeds signed 64-bit capacity. Canonical decimal TEXT can preserve values, but ordinary SQL arithmetic and ordering do not thereby gain decimal semantics. Decide query behavior before implementation; changing a typename alone is insufficient. [SQLite affinity](https://www.sqlite.org/datatype3.html), [SQLite floating point](https://www.sqlite.org/floatingpoint.html)
- **JSON:** The sole JSON override is NaturezasOperacao.JsonCfopsEscrituracao, a string containing a serialized List<int>. JSONB is a suitable proposed PostgreSQL representation for that use; it does not preserve whitespace, object key order or duplicate keys. Do not automatically convert existing TEXT fields merely because their member name contains Json. SQLite TEXT should receive a json_valid CHECK to preserve validation, with JSON functions available in the deployed SQLite build. SQLite JSONB is not PostgreSQL JSONB. [PostgreSQL JSON](https://www.postgresql.org/docs/current/datatype-json.html), [SQLite JSON functions](https://www.sqlite.org/json1.html)
- **Integers:** ServicosOs.Situacao is a CLR enum, not a string enum. Its current INT override is redundant with inference for this nonnullable enum. SQLite INTEGER permits a wider range than MySQL INT/PostgreSQL INTEGER; matching the 32-bit range strictly requires validation or a CHECK.

## Proposed API

Minimum enum for these declarations:

```csharp
public enum FieldType {
    Auto = 0,
    Text,
    MediumText,
    LongText,
    Blob,
    MediumBlob,
    Json,
    Int32,
    Decimal,
    Timestamp
}
```

Auto preserves inference from the CLR member type. Timestamp is included because Figlotech itself declares CreatedAt with Type = "TIMESTAMP". Additional explicit type members can be added when required; ordinary strings and numbers can continue using Auto and the existing Size metadata.

```csharp
[Field(Type = FieldType.MediumText, AllowNull = true)]
public string JsonBoleto { get; set; }

[Field(Type = FieldType.Decimal, DecimalPrecision = 26, DecimalScale = 12)]
public decimal Valor { get; set; }
```

Use separate numeric metadata. Existing FieldAttribute.Precision defaults to 3 and is used as **scale** in MySQL/PostgreSQL generators, despite the NUMERIC_PRECISION schema alias representing total digits. Introducing DecimalPrecision/DecimalScale avoids silently changing the meaning of existing Precision callers. Define fallback behavior for existing Auto decimal fields and support scale zero explicitly. Keep Size for length.

## Required integration points

1. **FieldAttribute and schema metadata:** Type is currently string, DATA_TYPE directly aliases it, and the (string tipo, string opcoes) constructor assigns it. Separate raw schema type metadata from the enum, preferably into a column metadata DTO; at minimum preserve an independent raw DatabaseType string. A database's type inventory must not be forced into the portable model enum. Changing Type to an enum is a public API break requiring a coordinated consumer/package migration; a staged alternative is a new enum property while deprecating the string property.
2. **All three QueryGenerators:** Resolve an explicit enum before CLR inference. GetDatabaseType should return an engine base/canonical name; GetDatabaseTypeWithLength should add length or precision/scale exactly once. Remove the existing raw override assignments from both methods. SQLite GetColumnDefinition contains a third override assignment that must also be removed. Preserve key/identity handling.
3. **StructureChecker:** It compares col.Type to GetDatabaseType using uppercased string equality and checks size only for VARCHAR/VARBINARY. PostgreSQL metadata uses UDT_NAME (e.g. int4), so emitting INTEGER without normalization would cause repeated alterations. Normalize equivalent names and compare decimal precision and scale separately. Mapping the text/binary tiers to the same target type should not trigger repeated schema changes. Current literal DECIMAL(p,s) overrides also mix a full declaration into a comparison against a base metadata type.
4. **JSON parameters:** The generic command builder binds strings as DbType.String. Npgsql requires explicit JSON/JSONB typing for string JSON writes (or an appropriate SQL cast); changing column DDL alone is insufficient. Cover INSERT, UPDATE and bulk/save paths. [Npgsql JSON mapping](https://www.npgsql.org/doc/types/json.html)
5. **Backup metadata:** BDadosBackupSchema currently copies field.Type into its string DatabaseType property. Define a stable representation for enum/precision/scale and preserve the ability to read older backup headers. Do not serialize an enum into this field incidentally.
6. **Figlotech's own overrides:** DataObject<T>.CreatedAt uses TIMESTAMP; _ResidualScData.Value uses BLOB. Migrate both. Timestamp needs an explicit timezone/storage policy for each provider; do not assume MySQL TIMESTAMP and PostgreSQL TIMESTAMP have identical timezone behavior.
7. **Validation before rollout:** Check every enum's base type, full column DDL, null/default options, precision zero scale, nullable CLR enums, JSON parameter writes, binary round trips, decimal boundaries and repeated structure checks against already-correct schemas. Include backup compatibility. This audit did not run builds or database tests because it changes documentation only.

Relevant current source locations:

- Figlotech.BDados/DataAccessAbstractions/Attributes/FieldAttribute.cs:22 (Type), :53 (DATA_TYPE), :62 (constructor).
- Figlotech.BDados.MySqlDataAccessor/MySqlQueryGenerator.cs:210 (type with length), :230 (base type).
- Figlotech.BDados.PostgreSQLDataAccessor/PgSQLQueryGenerator.cs:214 (type with length), :235 (base type).
- Figlotech.BDados.SQLiteDataAccessor/SqliteQueryGenerator.cs:116 (column definition), :149 (type with length), :162 (base type).
- Figlotech.BDados/DataAccessAbstractions/StructureChecker.cs:762 (raw type), :768 (type comparison input).
- Figlotech.BDados.PostgreSQLDataAccessor/PgSQLPlugin.cs:35 (UDT_NAME mapping).
- Figlotech.Core/Data/IQueryBuilder.cs:88 (string parameter binding).
- Figlotech.BDados/Helpers/BDadosBackupSchema.cs:37 (DatabaseType header value).
- Figlotech.BDados/DataAccessAbstractions/DataObject.cs:25 (TIMESTAMP override).
- Figlotech.BDados/DataAccessAbstractions/FTH_ScBackup.cs:11 (BLOB override).

## Complete ErpSoftLeader occurrence inventory

Paths below are relative to the ErpSoftLeader repository root; line numbers refer to the audited working tree.

| MySQL override | Source file:line | Member | CLR type |
| --- | --- | --- | --- |
| BLOB | ErpSoftLeader.Business/Models/Aggregates/Estoque.cs:1932 | DadosPrecosPorQuantidade | byte[] |
| BLOB | ErpSoftLeader.Business/Models/CmsCampanhas.cs:345 | BlobConfigSegmentacao | byte[] |
| BLOB | ErpSoftLeader.Business/Models/ComercialComandas.cs:354 | BlobAtribuicoes | byte[]? |
| BLOB | ErpSoftLeader.Business/Models/ComercialItensComandas.cs:187 | BlobControleAdicionais | byte[] |
| BLOB | ErpSoftLeader.Business/Models/ComercialPoliticasAgendamentos.cs:32 | BlobIntervalosPermitidos | byte[]? |
| BLOB | ErpSoftLeader.Business/Models/ComprasItensPedidos.cs:150 | DetNFe | byte[] |
| BLOB | ErpSoftLeader.Business/Models/ComprasPedidos.cs:204 | BlobNFe | byte[] |
| BLOB | ErpSoftLeader.Business/Models/ConfigsIntegracao.cs:51 | Dados | byte[] |
| BLOB | ErpSoftLeader.Business/Models/EmbalagensProdutos.cs:157 | DadosGrade | byte[]? |
| BLOB | ErpSoftLeader.Business/Models/Emitentes.cs:240 | BlobConfigsImpressaoDanfe | byte[] |
| BLOB | ErpSoftLeader.Business/Models/Emitentes.cs:268 | BlobUFsDestino | byte[] |
| BLOB | ErpSoftLeader.Business/Models/Emitentes.cs:302 | BlobSenhaCertificado | byte[] |
| BLOB | ErpSoftLeader.Business/Models/Emitentes.cs:306 | BlobConfigIntegracaoIFood | byte[] |
| BLOB | ErpSoftLeader.Business/Models/Emitentes.cs:310 | BlobAuthStateIntegracaoIFood | byte[] |
| BLOB | ErpSoftLeader.Business/Models/Emitentes.cs:314 | BlobCredenciaisImendes | byte[] |
| BLOB | ErpSoftLeader.Business/Models/Emitentes.cs:318 | BlobCredenciaisPOSControle | byte[] |
| BLOB | ErpSoftLeader.Business/Models/Emitentes.cs:380 | BlobConfigIntegracaoSicoob | byte[] |
| BLOB | ErpSoftLeader.Business/Models/Emitentes.cs:384 | BlobConfigIntegracaoEnroFintech | byte[] |
| BLOB | ErpSoftLeader.Business/Models/FinanceiroCentrosCusto.cs:20 | UnidadesArmazenagem | byte[] |
| BLOB | ErpSoftLeader.Business/Models/FinanceiroContasBancarias.cs:84 | UnidadesArmazenagem | byte[] |
| BLOB | ErpSoftLeader.Business/Models/FinanceiroSolicitacoesPix.cs:62 | SolicitacaoPixJsonGz | byte[] |
| BLOB | ErpSoftLeader.Business/Models/InfoCheckList.cs:42 | BlobItens | byte[] |
| BLOB | ErpSoftLeader.Business/Models/InfoExtrasNFe/InfoNFe.cs:181 | BlobDocumentosReferenciados | byte[] |
| BLOB | ErpSoftLeader.Business/Models/InfoExtrasNFe/InfoNFe.cs:209 | BlobHistoricoEventos | byte[] |
| BLOB | ErpSoftLeader.Business/Models/InfoExtrasNFe/InfoNFe.cs:237 | BlobVolumes | byte[] |
| BLOB | ErpSoftLeader.Business/Models/Produtos.cs:237 | ConfigGrade | byte[]? |
| BLOB | ErpSoftLeader.Business/Models/Produtos.cs:266 | DadosPrecosPorQuantidade | byte[] |
| BLOB | ErpSoftLeader.Business/Models/ProdutosAlteracoesPrecos.cs:64 | BlobUnidadesNaoAfetadas | byte[] |
| BLOB | ErpSoftLeader.Business/Models/ProdutosDadosSobreposicao.cs:65 | DadosPrecosPorQuantidade | byte[] |
| BLOB | ErpSoftLeader.Business/Models/ProdutosTemplatesGrade.cs:21 | Template | byte[] |
| BLOB | ErpSoftLeader.Business/Models/ServiceDeskTemplatesCheckList.cs:28 | BlobItens | byte[] |
| DECIMAL(18,2) | ErpSoftLeader.Business/Models/ComprasItensAdjudicacoes.cs:16 | PrecoCotado | decimal |
| DECIMAL(18,2) | ErpSoftLeader.Business/Models/ComprasItensPropostasIntegracao.cs:15 | Preco | decimal? |
| DECIMAL(18,2) | ErpSoftLeader.Business/Models/ComprasPedidos.cs:96 | ValorPedidoMinimoCotado | decimal? |
| DECIMAL(18,2) | ErpSoftLeader.Business/Models/ComprasPropostasIntegracao.cs:23 | ValorPedidoMinimo | decimal? |
| DECIMAL(18,3) | ErpSoftLeader.Business/Models/ComprasItensAdjudicacoes.cs:15 | Quantidade | decimal |
| DECIMAL(18,3) | ErpSoftLeader.Business/Models/ComprasItensPropostasIntegracao.cs:16 | QuantidadeOfertada | decimal? |
| DECIMAL(18,3) | ErpSoftLeader.Business/Models/ComprasItensPropostasIntegracao.cs:22 | FatorConversao | decimal? |
| DECIMAL(18,3) | ErpSoftLeader.Business/Models/ComprasItensPropostasIntegracao.cs:23 | MultiploVenda | decimal? |
| DECIMAL(18,6) | ErpSoftLeader.Business/Models/ComprasItensAdjudicacoes.cs:17 | PontuacaoPreco | decimal? |
| DECIMAL(18,6) | ErpSoftLeader.Business/Models/ComprasItensAdjudicacoes.cs:18 | PontuacaoPrazo | decimal? |
| DECIMAL(18,6) | ErpSoftLeader.Business/Models/ComprasItensAdjudicacoes.cs:19 | PontuacaoFinal | decimal? |
| DECIMAL(26,12) | ErpSoftLeader.Business/Models/ComprasItensPedidos.cs:81 | Valor | decimal |
| INT | ErpSoftLeader.Business/Models/ServicosOs.cs:57 | Situacao | SituacaoOS |
| JSON | ErpSoftLeader.Business/Models/NaturezasOperacao.cs:48 | JsonCfopsEscrituracao | string |
| LONGTEXT | ErpSoftLeader.Business/Models/ComprasAdjudicacoes.cs:18 | RequestEspelhoCongeladoJson | string? |
| LONGTEXT | ErpSoftLeader.Business/Models/ComprasCotacoesIntegracao.cs:36 | ProjecaoEstadoAtualJson | string? |
| LONGTEXT | ErpSoftLeader.Business/Models/ComprasCotacoesIntegracao.cs:37 | EncerramentoSolicitadoJson | string? |
| MEDIUMBLOB | ErpSoftLeader.Business/Models/CmsCampanhas.cs:90 | BlobImagem | byte[]? |
| MEDIUMBLOB | ErpSoftLeader.Business/Models/DocumentosImportados.cs:45 | Dados | byte[] |
| MEDIUMBLOB | ErpSoftLeader.Business/Models/Emitentes.cs:298 | BlobCertificado | byte[] |
| MEDIUMBLOB | ErpSoftLeader.Business/Models/FinanceiroHistoricoTitulos.cs:13 | DadosTitulo | byte[] |
| MEDIUMBLOB | ErpSoftLeader.Business/Models/Imagens.cs:23 | BlobData | byte[] |
| MEDIUMBLOB | ErpSoftLeader.Business/Models/OAuthClients.cs:42 | ScopesBlob | byte[] |
| MEDIUMTEXT | ErpSoftLeader.Business/Models/Auditoria.cs:191 | Dados | string? |
| MEDIUMTEXT | ErpSoftLeader.Business/Models/Boletos.cs:70 | JsonBoleto | string |
| MEDIUMTEXT | ErpSoftLeader.Business/Models/CmsCampanhas.cs:59 | Mensagem | string? |
| MEDIUMTEXT | ErpSoftLeader.Business/Models/ComercialRegistroStatusComanda.cs:21 | Observacoes | string? |
| MEDIUMTEXT | ErpSoftLeader.Business/Models/ComprasCotacoes.cs:49 | JsonFornecedoresAdicionais | string |
| MEDIUMTEXT | ErpSoftLeader.Business/Models/ComprasCotacoes.cs:72 | JsonContatos | string |
| MEDIUMTEXT | ErpSoftLeader.Business/Models/FinanceiroTurnosCaixa.cs:36 | ObservacoesFechamento | string? |
| MEDIUMTEXT | ErpSoftLeader.Business/Models/FinanceiroTurnosCaixa.cs:39 | ObservacoesConferencia | string? |
| MEDIUMTEXT | ErpSoftLeader.Business/Models/GuiasGNRe.cs:107 | XmlConteudo | string? |
| MEDIUMTEXT | ErpSoftLeader.Business/Models/InfoCheckList.cs:32 | Observacoes | string? |
| MEDIUMTEXT | ErpSoftLeader.Business/Models/InfoImagens.cs:34 | JsonDados | string |
| MEDIUMTEXT | ErpSoftLeader.Business/Models/KVUsuario.cs:20 | Valor | string |
| MEDIUMTEXT | ErpSoftLeader.Business/Models/Usuarios.cs:57 | JsonConfigDashboards | string |
| TEXT | ErpSoftLeader.Business/Models/Auditoria.cs:185 | Observacoes | string? |
| TEXT | ErpSoftLeader.Business/Models/Auditoria.cs:188 | Motivo | string? |
| TEXT | ErpSoftLeader.Business/Models/Boletos.cs:60 | MensagemErro | string? |
| TEXT | ErpSoftLeader.Business/Models/CmsComentarios.cs:23 | Conteudo | string? |
| TEXT | ErpSoftLeader.Business/Models/CmsEventos.cs:26 | Conteudo | string? |
| TEXT | ErpSoftLeader.Business/Models/CmsPostagens.cs:32 | Titulo | string? |
| TEXT | ErpSoftLeader.Business/Models/CmsPostagens.cs:36 | Conteudo | string? |
| TEXT | ErpSoftLeader.Business/Models/ComercialCancelamentosContratosAluguel.cs:26 | Motivo | string? |
| TEXT | ErpSoftLeader.Business/Models/ComercialComandas.cs:301 | DadosPrecosPorQuantidade | string? |
| TEXT | ErpSoftLeader.Business/Models/ComercialComandas.cs:489 | Observacoes | string? |
| TEXT | ErpSoftLeader.Business/Models/ComercialComandas.cs:492 | ObservacaoEspecialPix | string |
| TEXT | ErpSoftLeader.Business/Models/ComercialConsignacoes.cs:98 | ObservacoesAcerto | string |
| TEXT | ErpSoftLeader.Business/Models/ComercialConsignacoes.cs:101 | JsonDespesas | string |
| TEXT | ErpSoftLeader.Business/Models/ComercialConsignacoes.cs:130 | JsonConfigComissao | string |
| TEXT | ErpSoftLeader.Business/Models/ComercialContratosAluguel.cs:174 | Observacoes | string? |
| TEXT | ErpSoftLeader.Business/Models/ComercialItensComandas.cs:68 | Observacoes | string? |
| TEXT | ErpSoftLeader.Business/Models/ComercialItensContratosAlguel.cs:24 | Detalhes | string? |
| TEXT | ErpSoftLeader.Business/Models/ComercialItensPedidos.cs:27 | Detalhes | string? |
| TEXT | ErpSoftLeader.Business/Models/ComercialOrcamentosOsItens.cs:32 | DefeitosApresentados | string? |
| TEXT | ErpSoftLeader.Business/Models/ComercialOrcamentosOsItens.cs:35 | Detalhes | string? |
| TEXT | ErpSoftLeader.Business/Models/ComercialPedidos.cs:75 | Observacoes | string? |
| TEXT | ErpSoftLeader.Business/Models/ComercialValeCompras.cs:47 | Observacoes | string? |
| TEXT | ErpSoftLeader.Business/Models/ComercialVendas.cs:121 | DadosPrecosPorQuantidade | string? |
| TEXT | ErpSoftLeader.Business/Models/ComercialVendas.cs:139 | DadosPedidosShipay | string? |
| TEXT | ErpSoftLeader.Business/Models/ComercialVendas.cs:224 | Observacoes | string? |
| TEXT | ErpSoftLeader.Business/Models/ComercialVendas.cs:227 | ObservacaoEspecialPix | string |
| TEXT | ErpSoftLeader.Business/Models/ComprasAdjudicacoes.cs:17 | IdsPropostasRevisaoReconhecidaJson | string? |
| TEXT | ErpSoftLeader.Business/Models/ComprasCotacoes.cs:42 | Observacoes | string? |
| TEXT | ErpSoftLeader.Business/Models/ComprasItensCotacoes.cs:26 | JsonQuantidadesUA | string? |
| TEXT | ErpSoftLeader.Business/Models/ComprasItensPropostas.cs:39 | JsonFatorConversao | string? |
| TEXT | ErpSoftLeader.Business/Models/ComprasPedidos.cs:152 | Observacoes | string? |
| TEXT | ErpSoftLeader.Business/Models/ComprasPropostas.cs:31 | Observacoes | string? |
| TEXT | ErpSoftLeader.Business/Models/Compromissos.cs:89 | Descricao | string? |
| TEXT | ErpSoftLeader.Business/Models/DistribuicaoItensPedidos.cs:44 | Detalhes | string? |
| TEXT | ErpSoftLeader.Business/Models/DistribuicaoItensPedidos.cs:50 | JsonQuantidadesMultiLoja | string? |
| TEXT | ErpSoftLeader.Business/Models/DistribuicaoPedidos.cs:75 | Observacoes | string? |
| TEXT | ErpSoftLeader.Business/Models/Emitentes.cs:167 | TextoRodape | string? |
| TEXT | ErpSoftLeader.Business/Models/Emitentes.cs:170 | ObservacoesPadraoNFe | string? |
| TEXT | ErpSoftLeader.Business/Models/FinanceiroCuponsTEF.cs:30 | JsonTransacao | string? |
| TEXT | ErpSoftLeader.Business/Models/FinanceiroCuponsTEF.cs:33 | Impressao | string? |
| TEXT | ErpSoftLeader.Business/Models/FinanceiroMeiosPagamento.cs:285 | ConfigAdicional | string? |
| TEXT | ErpSoftLeader.Business/Models/FinanceiroParcelas.cs:303 | PagamentosConsolidados | string? |
| TEXT | ErpSoftLeader.Business/Models/FinanceiroParcelas.cs:351 | DadosPedidoShipay | string? |
| TEXT | ErpSoftLeader.Business/Models/FinanceiroTentativasDirectPin.cs:23 | JsonResultado | string |
| TEXT | ErpSoftLeader.Business/Models/FinanceiroTitulos.cs:134 | JsonAjustes | string |
| TEXT | ErpSoftLeader.Business/Models/FinanceiroTitulos.cs:189 | Observacoes | string? |
| TEXT | ErpSoftLeader.Business/Models/InfoDispositivosMoveis.cs:117 | Diagnostico | string? |
| TEXT | ErpSoftLeader.Business/Models/NaturezasOperacao.cs:79 | BlobConversaoCFOP | string? |
| TEXT | ErpSoftLeader.Business/Models/Pessoas.cs:123 | Observacoes | string? |
| TEXT | ErpSoftLeader.Business/Models/Pessoas.cs:129 | MotivoBloqueio | string? |
| TEXT | ErpSoftLeader.Business/Models/PessoasDocumentos.cs:35 | Observacoes | string? |
| TEXT | ErpSoftLeader.Business/Models/Produtos.cs:221 | Observacoes | string? |
| TEXT | ErpSoftLeader.Business/Models/ProdutosAjustesPreco.cs:108 | JsonGruposPrecificacao | string |
| TEXT | ErpSoftLeader.Business/Models/ProdutosGruposPrecosPorUnidade.cs:23 | JsonUnidades | string |
| TEXT | ErpSoftLeader.Business/Models/ProdutosItensIntegracoes.cs:31 | JsonObjRemotoAtual | string? |
| TEXT | ErpSoftLeader.Business/Models/ServicosOsItens.cs:34 | DefeitosApresentados | string? |
| TEXT | ErpSoftLeader.Business/Models/ServicosOsItens.cs:37 | Detalhes | string? |
| TEXT | ErpSoftLeader.Business/Models/SistemaObservacoes.cs:19 | Observacoes | string? |
| TEXT | ErpSoftLeader.Business/Models/UnidadesArmazenagem.cs:36 | ContasFechamentoCaixa | string? |
| TEXT | ErpSoftLeader.Core/Models/LoggerException.cs:40 | Message | string |
| TEXT | ErpSoftLeader.Core/Models/LoggerException.cs:43 | StackTrace | string |
