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
     */
    public function fetch(ResultSetDescriptorInterface $resultSetDescriptor): ProjectionInterface
    {
        $query = $this->getClient()->createSelect();

        $filters = $resultSetDescriptor->getFilters();
        foreach ($filters as $filter) {
            switch ($filter['operator']) {
                default:
                    $filterQuery = $filter['property'] . ':' . $filter['value'];
                    break;
            }

            $query->createFilterQuery($filter['property'])->setQuery($filterQuery);
        }

        if ($size = $resultSetDescriptor->getSize()) {
            $this->paginateNextQuery = false;
            $query->setStart(0)->setRows($size);
        } else if ($page = $resultSetDescriptor->getPage()) {
            $this->paginate($page, $resultSetDescriptor->getPageSize());
        }


        return $this->query($query, self::FETCH_PROJECTION);
    }

    /**
     * @return Client
     */
    public function getClient(): Client
    {
        return $this->client;
    }

    /**
     * @param Client $client
     *
     * @return AbstractSolrGateway
     */
    public function setClient(Client $client): AbstractSolrGateway
    {
        $this->client = $client;

        return $this;
    }

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

    protected function preparePagination(QueryInterface $query)
    {
        if ($this->paginateNextQuery && $query instanceof Query) {
            $start = ($this->currentPage - 1) * $this->pageSize;
            $query->setStart($start)->setRows($this->pageSize);
        }
    }

    protected function buildResultSet(ResultInterface $result): ResultSetInterface
    {
        $resultSet = ($this->paginateNextQuery) ? new PaginatedResultSet() : new ResultSet();

        if ($resultSet instanceof PaginatedResultSetInterface) {
            $resultSet->setCurrentPage($this->currentPage)->setPerPage($this->pageSize)->setTotal(
                $result->getNumFound()
            )
            ;
        }

        /** @var Document $document */
        foreach ($result->getDocuments() as $document) {
            $entity      = $this->entityFactory($document->getFields());
            $resultSet[] = $entity;
        }

        return $resultSet;
    }

    protected function buildProjection(ResultInterface $result): ProjectionInterface
    {
        $resultSet = ($this->paginateNextQuery) ? new PaginatedProjection() : new Projection();

        if ($resultSet instanceof PaginatedProjectionInterface) {
            $resultSet->setCurrentPage($this->currentPage)->setPerPage($this->pageSize)->setTotal(
                $result->getNumFound()
            )
            ;
        }

        $hydrator = $this->getHydrator();

        /** @var Document $document */
        $documents = $result->getDocuments();
        foreach ($documents as $document) {
            $documentFields = $document->getFields();
            unset($documentFields['score']);
            unset($documentFields['_version_']);
            $entity = new Entity();
            $hydrator->hydrate($documentFields, $entity);
            $resultSet[] = $entity;
        }

        return $resultSet;
    }

    /**
     * @param ResultSetDescriptorInterface $resultSetDescriptor
     *
     * @return ResultSetInterface
     */
    public function fetchAll(ResultSetDescriptorInterface $resultSetDescriptor): ResultSetInterface
    {
        $query = $this->getClient()->createSelect();

        $filters = $resultSetDescriptor->getFilters();
        foreach ($filters as $filter) {
            switch ($filter['operator']) {

                default:
                    $filterQuery = $filter['property'] . ':' . $filter['value'];
                    break;
            }

            $query->createFilterQuery($filter['property'])->setQuery($filterQuery);
        }

        if ($size = $resultSetDescriptor->getSize()) {
            $this->paginateNextQuery = false;
            $query->setStart(0)->setRows($size);
        } else if ($page = $resultSetDescriptor->getPage()) {
            $this->paginate($page, $resultSetDescriptor->getPageSize());
        }

        if($sort = $resultSetDescriptor->getSort())
        {
            foreach($sort as $property => $direction)
            {
                $query->addSort($property, $direction);
            }
        }

        return $this->query($query, self::FETCH_ENTITIES);
    }

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
     * @throws SolrGatewayException
     */
    public function purge(ResultSetDescriptorInterface $resultSetDescriptor)
    {
        throw new SolrGatewayException('purge() method is not handled by this gateway yet');
    }

    /**
     * @param EntityInterface $entity
     *
     * @throws GatewayException
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

                $result = $this->getClient()->update($update);
            } catch
            (\Exception $e) {
                throw new SolrGatewayException($e->getMessage(), $e->getCode(), $e);
            }
        }

        return true;

    }

    /**
     * @param EntityInterface $entity
     *
     * @throws GatewayException
     */
    public
    function delete(
        EntityInterface ...$entities
    ): bool {
        throw new GatewayException('Not implemented yet');
    }

    /**
     *
     */
    public
    function triggerDeltaImport()
    {
        $request = new Request();
        $request->setHandler('dataimport');
        $request->addParam('command', 'delta-import');

        $this->getClient()->executeRequest($request);
    }

}
