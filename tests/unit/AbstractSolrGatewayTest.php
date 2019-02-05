<?php

namespace Tests\ObjectivePHP\Gateway\SolR;

use ObjectivePHP\Gateway\Projection\Projection;
use ObjectivePHP\Gateway\ResultSet\Descriptor\ResultSetDescriptor;
use ObjectivePHP\Gateway\ResultSet\Descriptor\ResultSetDescriptorInterface;
use ObjectivePHP\Gateway\SolR\AbstractSolrGateway;
use Solarium\QueryType\Select\Result\Result;
use PHPUnit\Framework\TestCase;
use Solarium\Client;

/**
 * Class AbstractSolrGatewayTest
 *
 * @package Tests\ObjectivePHP\Gateway\SolR\Exception
 */
class AbstractSolrGatewayTest extends TestCase
{
    /**
     * @dataProvider dataForTestResultSetDescriptorWithMultiFilterQueryKey
     *
     * @param string $result
     * @param array  $filters
     */
    public function testResultSetDescriptorWithMultiFilterQueryKey(string $result, array $filters)
    {
        $descriptor = new ResultSetDescriptor('test');

        foreach ($filters as $filter) {
            $descriptor->addFilter('property', $filter[0], $filter[1]);
        }

        $gateway = $this->getMockBuilder(AbstractSolrGateway::class)->setMethods(['query'])->getMock();

        $gateway->setClient(new Client());

        $query = new Client();
        $select = $query->createSelect();
        $select->getHelper();

        $select->createFilterQuery('property')->setQuery('property:' . $result);

        $gateway->expects($this->once())->method('query')->with($select)->willReturn(new Projection());

        $gateway->fetch($descriptor);
    }

    public function dataForTestResultSetDescriptorWithMultiFilterQueryKey()
    {
        return [
            0 => [
                '["value1" TO "value2"]',
                [['value1', ResultSetDescriptorInterface::OP_GTOE], ['value2', ResultSetDescriptorInterface::OP_LTOE]]
            ],
            1 => [
                '{"value1" TO "value2"]',
                [['value1', ResultSetDescriptorInterface::OP_GT], ['value2', ResultSetDescriptorInterface::OP_LTOE]]
            ],
            2 => [
                '{"value1" TO "value2"}',
                [['value1', ResultSetDescriptorInterface::OP_GT], ['value2', ResultSetDescriptorInterface::OP_LT]]
            ],
            3 => [
                '["value1" TO "value2"}',
                [['value1', ResultSetDescriptorInterface::OP_GTOE], ['value2', ResultSetDescriptorInterface::OP_LT]]
            ],
            4 => [
                '["value1" TO *]',
                [['value1', ResultSetDescriptorInterface::OP_GTOE]]
            ],
            5 => [
                '{"value1" TO *]',
                [['value1', ResultSetDescriptorInterface::OP_GT]]
            ],
            6 => [
                '[* TO "value1"]',
                [['value1', ResultSetDescriptorInterface::OP_LTOE]]
            ],
            7 => [
                '[* TO "value1"}',
                [['value1', ResultSetDescriptorInterface::OP_LT]]
            ]
        ];
    }

    public function testSortDescription()
    {
        $descriptor = new ResultSetDescriptor('test');
        $descriptor->sort('property1', ResultSetDescriptor::SORT_ASC);
        $descriptor->sort('property2', ResultSetDescriptor::SORT_DESC);

        $query = new Client();
        $select = $query->createSelect();

        $select->addSort('property1', $select::SORT_ASC);
        $select->addSort('property2', $select::SORT_DESC);

        $gateway = $this->getMockBuilder(AbstractSolrGateway::class)->setMethods(['query'])->getMock();
        $gateway->setClient(new Client());

        $gateway->expects($this->once())->method('query')->with($select)->willReturn(new Projection());

        $gateway->fetch($descriptor);
    }

    public function testPaginationDescription()
    {
        $descriptor = new ResultSetDescriptor('test');
        $descriptor->paginate(3, 32);

        $query = new Client();
        $select = $query->createSelect();

        $select->setRows(32);
        $select->setStart(64);

        $gateway = new class extends AbstractSolrGateway {
        };

        $result = $this->createMock(Result::class);
        $result->expects($this->once())->method('getNumFound')->willReturn(32);
        $result->expects($this->once())->method('getDocuments')->willReturn([]);

        $client = $this->getMockBuilder(Client::class)->getMock();
        $client->expects($this->once())->method('createSelect')->willReturn($query->createSelect());
        $client->expects($this->once())->method('execute')->with($select)->willReturn($result);

        $gateway->setClient($client);

        $gateway->fetch($descriptor);
    }
}
