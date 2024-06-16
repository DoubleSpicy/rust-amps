#include <amps/ampsplusplus.hpp>
#include <iostream>
#include <string>
#include "sample.pb.h"

using namespace std;

int main()
{
  const char* uri = "tcp://127.0.0.1:9010/amps";

  AMPS::Client ampsClient("sampleProtobufPublisher");
  try
  {
    ampsClient.connect(uri);
    ampsClient.logon();

    string data;
    Test::Order test_order;

    test_order.set_customername("Bob");
    test_order.set_customerid(60);
    test_order.set_address("123 Foo rd");
    test_order.add_product("water");
    test_order.add_product("pizza");
    test_order.SerializeToString(&data);
    ampsClient.publish("messages", data);

    test_order.Clear();

    test_order.set_customername("Jim");
    test_order.set_customerid(61);
    test_order.add_product("water");
    test_order.SerializeToString(&data);
    ampsClient.publish("messages", data);
  }
  catch (const AMPS::AMPSException& e)
  {
    cout << "Error: " << e.what() << endl;
  }
  return 0;
}
