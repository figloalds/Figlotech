// Decompiled with JetBrains decompiler
// Type: CmcSoftLeader.Models.itens_comanda
// Assembly: cmc-softleader-business, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null
// MVID: FD9AACBB-6E4C-4675-ACCA-909DCCDFDD78
// Assembly location: C:\Users\felyp\Dropbox\Aplicativos\Azure\cmc-softleader\cmc-softleader-business.dll

using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.BDados.DataAccessAbstractions;

namespace SoftLeader.Sistema.Models
{
  public class itens_comanda : SoftLeaderDataObject
  {
    [PrimaryKey][ReliableId]
    [Field(PrimaryKey = true)]
    public long ico_id;
    [Field]
    public long cm_id;
    [Field(AllowNull = true)]
    public float? ico_vlrunit;
    [Field(DefaultValue = 0)]
    public int ico_qtde;
    [Field]
    public float ico_vlrtotal;
    [Field(AllowNull = true, Size = 1)]
    public string ico_tipo;
    [Field(AllowNull = true, Size = 100)]
    public string ico_desc;
    [Field(AllowNull = true)]
    public long? pro_id;
    [Field(AllowNull = true)]
    public long? col_id;
    [Field(AllowNull = true)]
    public float? est_pcusto;
  }
}
