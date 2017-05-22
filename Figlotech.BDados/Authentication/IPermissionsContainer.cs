namespace Figlotech.BDados.Authentication
{
    /// <summary>
    /// Short for Access Control Level
    /// </summary>
    public enum Acl {
        Create      = 0,
        Read        = 1,
        Update      = 2,        
        Delete      = 3,
        Authorize   = 4,
    }
    /// <summary>
    /// Defines an object that carries a permission buffer.
    /// You can socket this right into your User AND/OR Roles dataObjects
    /// BDados does the Magic of getting/setting the right permissions into these bytes.
    /// </summary>
    public interface IPermissionsContainer {
        /// <summary>
        /// This is supposed to be a sum of BDadosPermissions
        /// It's a C-Like additive tag 
        /// </summary>
        byte[] Buffer { get; set; }
    }
}
