#include <amps/ampsplusplus.hpp>
#include <iostream>
#include <string>
#include "sample.pb.h"

using namespace std;

int main()
{
  const char* uri = "tcp://127.0.0.1:9010/amps";
  AMPS::Client ampsClient("sampleProtoBufSubscriber");
  ampsClient.connect(uri);
  ampsClient.logon();

  Test::Order test_order;
  try
  {
    ampsClient.connect(uri);
    ampsClient.logon();

    for (auto message : ampsClient.execute(AMPS::Command("subscribe") \
                                           .setTopic("messages").setFilter("/Address = '123 Foo rd'")))
    {
      if (message.getData().empty())
      {
        continue;
      }

      test_order.ParseFromString(message.getData());
      cout << endl << "CustomerName: " << test_order.customername() << endl;
      cout << "CustomerID: " << test_order.customerid() << endl;
      cout << "Address: "  << test_order.address() << endl;

      for (int i = 0; i < test_order.product_size(); ++i)
      {
        cout << "Product: " << test_order.product(i) << endl;
      }
    }
  }
  catch (const AMPS::AMPSException& e)
  {
    cerr << "Error: " << e.what() << endl;
  }

  return 0;
}
