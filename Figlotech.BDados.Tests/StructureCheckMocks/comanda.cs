// Decompiled with JetBrains decompiler
// Type: CmcSoftLeader.Models.comanda
// Assembly: cmc-softleader-business, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null
// MVID: FD9AACBB-6E4C-4675-ACCA-909DCCDFDD78
// Assembly location: C:\Users\felyp\Dropbox\Aplicativos\Azure\cmc-softleader\cmc-softleader-business.dll

using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.BDados.DataAccessAbstractions;
using System;

namespace SoftLeader.Sistema.Models {
    public class comanda : SoftLeaderDataObject {
        [PrimaryKey][ReliableId]
        [Field(PrimaryKey = true)]
        public long cm_id;
        [Field]
        public long col_id;
        [Field(AllowNull = true, Size = 255)]
        public string cm_descricao;
        [Field(DefaultValue = "N", Size = 1)]
        public string cm_status;
        [Field(AllowNull = true)]
        public long? cm_terminal;
        [Field(AllowNull = true)]
        public int? cli_id;
        [Field(DefaultValue = "CURRENT_TIMESTAMP")]
        public DateTime cm_abertura;
        [Field(AllowNull = true)]
        public DateTime? cm_fechamento;
        [Field(AllowNull = true, Size = 1)]
        public string cm_stentrega;
        [Field(AllowNull = true, Size = 1)]
        public string cm_stpagamento;
        [Field(AllowNull = true)]
        public long? mc_id;
        [Field(DefaultValue = 0.0)]
        public float cm_total;
        [Field(AllowNull = true, Size = 60)]
        public string cm_cartao;
        [Field(DefaultValue = 0.0)]
        public float cm_saldoTrocavel;
        [Field(DefaultValue = 0.0)]
        public float cm_saldoNaoTrocavel;
        [Field(DefaultValue = 0.0)]
        public float cm_saldo;
        [Field(DefaultValue = true)]
        public bool cm_modoPre;
        [Field(DefaultValue = 0.0)]
        public float cm_subtotal;
        [Field(DefaultValue = 0.0)]
        public float cm_taxaServico;
        [Field(AllowNull = true, Size = 400)]
        public string cm_obs;

        [AggregateList(typeof(itens_comanda), "cm_id")]
        public RecordSet<itens_comanda> cm_itenscomanda;
    }
}
