namespace IntegratedS3.Tests;

internal static class ChecksumTestAlgorithms
{
    public static string ComputeSha1Base64(string content) => IntegratedS3.Testing.ChecksumTestAlgorithms.ComputeSha1Base64(content);

    public static string ComputeSha256Base64(string content) => IntegratedS3.Testing.ChecksumTestAlgorithms.ComputeSha256Base64(content);

    public static string ComputeCrc32cBase64(string content) => IntegratedS3.Testing.ChecksumTestAlgorithms.ComputeCrc32cBase64(content);

    public static string ComputeMultipartCrc32cBase64(params string[] partChecksums) => IntegratedS3.Testing.ChecksumTestAlgorithms.ComputeMultipartCrc32cBase64(partChecksums);
}
