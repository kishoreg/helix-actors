package org.apache.helix.ipc.benchmark;

public class RunBenchmark {
  public static void main(String[] args) throws Exception{
    TestServer.main(new String[]{"8011", "localhost", "8012"});
    TestServer.main(new String[]{"8012", "localhost", "8011"});
  }
}
