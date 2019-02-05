<?php

namespace ObjectivePHP\Gateway\SolR;

use ObjectivePHP\Gateway\AbstractPaginableGateway;
use ObjectivePHP\Gateway\Entity\Entity;
use ObjectivePHP\Gateway\Entity\EntityInterface;
use ObjectivePHP\Gateway\Exception\GatewayException;
use ObjectivePHP\Gateway\Hydrator\DenormalizedDataExtractorInterface;
use ObjectivePHP\Gateway\Projection\PaginatedProjection;
use ObjectivePHP\Gateway\Projection\PaginatedProjectionInterface;
use ObjectivePHP\Gateway\Projection\Projection;
use ObjectivePHP\Gateway\Projection\ProjectionInterface;
use ObjectivePHP\Gateway\ResultSet\Descriptor\ResultSetDescriptorInterface;
use ObjectivePHP\Gateway\ResultSet\PaginatedResultSet;
use ObjectivePHP\Gateway\ResultSet\PaginatedResultSetInterface;
use ObjectivePHP\Gateway\ResultSet\ResultSet;
use ObjectivePHP\Gateway\ResultSet\ResultSetInterface;
use ObjectivePHP\Gateway\SolR\Exception\SolrGatewayException;
use Solarium\Client;
use Solarium\Core\Client\Request;
use Solarium\Core\Query\QueryInterface;
use Solarium\Core\Query\Result\ResultInterface;
use Solarium\QueryType\Select\Query\Query;
use Solarium\QueryType\Select\Result\Document;
use Solarium\QueryType\Select\Result\Result;

/**
 * Class AbstractSolrGateway
 *
 * @package ObjectivePHP\Gateway\SolR
 */
abstract class AbstractSolrGateway extends AbstractPaginableGateway
{
    /**
     * Solr client.
     *
     * @var Client
     */
    protected $client;

    /**
     * @param ResultSetDescriptorInterface $resultSetDescriptor
     *
     * @return ProjectionInterface
     * @throws GatewayException
     */
    public function fetch(ResultSetDescriptorInterface $resultSetDescriptor): ProjectionInterface
    {
        return $this->query(
            $this->buildQuery($this->getClient()->createSelect(), $resultSetDescriptor),
            self::FETCH_PROJECTION
        );
    }

    /**
     * @param ResultSetDescriptorInterface $resultSetDescriptor
     *
     * @return ResultSetInterface
     * @throws GatewayException
     */
    public function fetchAll(ResultSetDescriptorInterface $resultSetDescriptor): ResultSetInterface
    {
        return $this->query(
            $this->buildQuery($this->getClient()->createSelect(), $resultSetDescriptor),
            self::FETCH_ENTITIES
        );
    }

    /**
     * @param QueryInterface $query
     * @param int            $mode
     *
     * @return ProjectionInterface|ResultSetInterface
     * @throws GatewayException
     */
    public function query(QueryInterface $query, $mode = self::FETCH_ENTITIES)
    {
        $this->preparePagination($query);

        /** @var Result $result */
        $result = $this->getClient()->execute($query);

        switch ($mode) {
            case self::FETCH_ENTITIES:
                return $this->buildResultSet($result);

            case self::FETCH_PROJECTION:
                return $this->buildProjection($result);

            default:
                throw new GatewayException(sprintf('Unknown query mode "%s"', $mode));
        }
    }

    /**
     * @param QueryInterface $query
     */
    protected function preparePagination(QueryInterface $query)
    {
        if ($this->paginateNextQuery && $query instanceof Query) {
            $start = ($this->currentPage - 1) * $this->pageSize;
            $query->setStart($start)->setRows($this->pageSize);
        }
    }

    /**
     * Build Solr query from result set descriptor
     *
     * @param Query                        $query
     * @param ResultSetDescriptorInterface $resultSetDescriptor
     *
     * @return Query
     */
    protected function buildQuery(Query $query, ResultSetDescriptorInterface $resultSetDescriptor)
    {
        $filters = $resultSetDescriptor->getFilters();

        $ranges = [];

        foreach ($filters as $filter) {
            if ($filter['value'] instanceof \DateTimeInterface) {
                $filter['value'] = $filter['value']->format('Y-m-d\TH:i:s\Z');
            }

            $filter['value'] = $query->getHelper()->escapePhrase($filter['value']);

            switch ($filter['operator']) {
                case ResultSetDescriptorInterface::OP_GTOE:
                    $ranges[$filter['property']][0] = '[' . $filter['value'];
                    break;
                case ResultSetDescriptorInterface::OP_GT:
                    $ranges[$filter['property']][0] = '{' . $filter['value'];
                    break;
                case ResultSetDescriptorInterface::OP_LTOE:
                    $ranges[$filter['property']][1] = $filter['value'] . ']';
                    break;
                case ResultSetDescriptorInterface::OP_LT:
                    $ranges[$filter['property']][1] = $filter['value'] . '}';
                    break;
                default:
                    $filterQuery = $filter['property'] . ':' . $filter['value'];
                    $query->createFilterQuery($filter['property'])->setQuery($filterQuery);
                    break;
            }
        }

        foreach ($ranges as $property => $range) {
            if (count($range) == 2) {
                ksort($range);
                $query->createFilterQuery($property)->setQuery($property . ':' . implode(' TO ', $range));
            } elseif (count($range) == 1) {
                if (array_key_exists(1, $range)) {
                    $query->createFilterQuery($property)->setQuery($property . ':' . sprintf('[* TO %s', $range[1]));
                } elseif (array_key_exists(0, $range)) {
                    $query->createFilterQuery($property)->setQuery($property . ':' . sprintf('%s TO *]', $range[0]));
                }
            }
        }

        if ($sort = $resultSetDescriptor->getSort()) {
            foreach ($sort as $property => $direction) {
                $query->addSort($property, $direction);
            }
        }

        if ($size = $resultSetDescriptor->getSize()) {
            $this->paginateNextQuery = false;
            $query->setStart(0)->setRows($size);
        } elseif ($page = $resultSetDescriptor->getPage()) {
            $this->paginate($page, $resultSetDescriptor->getPageSize());
        }

        return $query;
    }

    /**
     * @param ResultInterface $result
     *
     * @return ResultSetInterface
     * @throws GatewayException
     */
    protected function buildResultSet(ResultInterface $result): ResultSetInterface
    {
        $resultSet = $this->paginateNextQuery ? new PaginatedResultSet() : new ResultSet();

        if ($resultSet instanceof PaginatedResultSetInterface) {
            $resultSet->setCurrentPage($this->currentPage)->setPerPage($this->pageSize)->setTotal(
                $result->getNumFound()
            );
        }

        /** @var Document $document */
        foreach ($result->getDocuments() as $document) {
            $entity = $this->entityFactory($document->getFields());
            $resultSet[] = $entity;
        }

        return $resultSet;
    }

    /**
     * @param ResultInterface $result
     *
     * @return ProjectionInterface
     */
    protected function buildProjection(ResultInterface $result): ProjectionInterface
    {
        $resultSet = $this->paginateNextQuery ? new PaginatedProjection() : new Projection();

        if ($resultSet instanceof PaginatedProjectionInterface) {
            $resultSet->setCurrentPage($this->currentPage)->setPerPage($this->pageSize)->setTotal(
                $result->getNumFound()
            );
        }

        $hydrator = $this->getHydrator();

        /** @var Document $document */
        $documents = $result->getDocuments();
        foreach ($documents as $document) {
            $documentFields = $document->getFields();
            unset($documentFields['score'], $documentFields['_version_']);
            $entity = new Entity();
            $hydrator->hydrate($documentFields, $entity);
            $resultSet[] = $entity;
        }

        return $resultSet;
    }

    /**
     * @param $key
     *
     * @return EntityInterface
     * @throws GatewayException
     */
    public function fetchOne($key): EntityInterface
    {
        $query = $this->getClient()->createSelect();
        $query->createFilterQuery('id')->setQuery('id:' . $key);

        $result = $this->query($query, self::FETCH_ENTITIES);

        return $result[0];
    }

    /**
     * @param ResultSetDescriptorInterface $descriptor
     * @param mixed                        $data
     *
     * @throws SolrGatewayException
     */
    public function update(ResultSetDescriptorInterface $descriptor, $data)
    {
        throw new SolrGatewayException('update() method is not handled by this gateway yet');
    }

    /**
     * @param ResultSetDescriptorInterface $resultSetDescriptor
     *
     * @return \Solarium\QueryType\Update\Result
     */
    public function purge(ResultSetDescriptorInterface $resultSetDescriptor)
    {
        $client = $this->getClient();

        $update = $client->createUpdate();
        $query = $update->addDeleteQuery('*:*');
        $query->addCommit();

        return $client->update($query);
    }

    /**
     * @param EntityInterface $entity
     *
     * @return bool
     * @throws SolrGatewayException
     */
    public function create(EntityInterface $entity): bool
    {
        $update = $this->getClient()->createUpdate();

        if (!$this->getOptions()['id']) {
            throw new SolrGatewayException(
                'Missing ID to create an entity. Did you forget to call Metagateway::setNewId ?'
            );
        }

        $entity[$entity->getEntityIdentifier()] = $this->getOptions()['id'];

        try {
            $hydrator = $this->getHydrator();

            if ($hydrator instanceof DenormalizedDataExtractorInterface) {
                $data = $hydrator->extractDenormalized($entity);
            } else {
                $data = $hydrator->extract($entity);
            }

            foreach ($data as $field => &$value) {
                if ($value instanceof \DateTime) {
                    $value = $value->format(\DateTime::ATOM);
                }
            }

            $update->addDocument($update->createDocument($data));
            $update->addCommit();

            $this->getClient()->update($update);
        } catch (\Exception $e) {
            throw new SolrGatewayException($e->getMessage(), $e->getCode(), $e);
        }

        return true;
    }


    /**
     * @param EntityInterface[] $entities
     *
     * @return bool
     * @throws SolrGatewayException
     */
    public function persist(EntityInterface ...$entities): bool
    {
        $update = $this->getClient()->createUpdate();
        foreach ($entities as $entity) {
            try {
                $hydrator = $this->getHydrator();

                if ($hydrator instanceof DenormalizedDataExtractorInterface) {
                    $data = $hydrator->extractDenormalized($entity);
                } else {
                    $data = $hydrator->extract($entity);
                }

                foreach ($data as $field => &$value) {
                    if ($value instanceof \DateTime) {
                        $value = $value->format(\DateTime::ISO8601);
                    }
                }

                $update->addDocument($update->createDocument($data));
                $update->addCommit();
                $this->getClient()->update($update);

            } catch (\Exception $e) {
                throw new SolrGatewayException($e->getMessage(), $e->getCode(), $e);
            }
        }

        return true;
    }

    /**
     * @param EntityInterface[] $entities
     *
     * @return bool
     *
     * @throws SolrGatewayException
     */
    public function delete(EntityInterface ...$entities): bool
    {
        $update = $this->getClient()->createUpdate();
        $ids = [];

        foreach ($entities as $entity) {
            $ids[] = $entity[$entity->getEntityIdentifier()];
        }

        try {
            $query = $update->addDeleteByIds($ids);
            $query->addCommit();
            $this->getClient()->update($query);
        } catch (\Exception $e) {
            throw new SolrGatewayException($e->getMessage(), $e->getCode(), $e);
        }

        return true;
    }

    public function triggerDeltaImport()
    {
        $request = new Request();
        $request->setHandler('dataimport');
        $request->addParam('command', 'delta-import');

        $this->getClient()->executeRequest($request);
    }

    /**
     * Get Client
     *
     * @return Client
     */
    public function getClient(): Client
    {
        return $this->client;
    }

    /**
     * Set Client
     *
     * @param Client $client
     *
     * @return $this
     */
    public function setClient(Client $client)
    {
        $this->client = $client;

        return $this;
    }
}
